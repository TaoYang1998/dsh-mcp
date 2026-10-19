use crate::config::limits;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    AgentMessage,
    AgentThought,
    ToolCall,
    ToolCallUpdate,
    ConfigUpdate,
    Usage,
    StatusChange,
    PermissionRequested,
    PermissionResolved,
    PermissionAutoDenied,
    PermissionCancelled,
    BridgeError,
    CompatNote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Event {
    pub seq: u64,
    pub time: String,
    #[serde(rename = "type")]
    pub kind: EventKind,
    pub data: Value,
    pub truncated: bool,
}

pub fn rfc3339_now() -> String {
    rfc3339_from_unix(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}

pub fn rfc3339_from_unix(secs: u64) -> String {
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, mi, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

pub fn truncate_utf8(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

/// Returns the longest prefix whose JSON string encoding fits the budget.
/// A JSON string requires at least two bytes, even when empty.
pub fn truncate_json_text(s: &str, max_bytes: usize) -> (&str, bool) {
    let mut used = 2usize;
    let mut end = 0;
    for (index, c) in s.char_indices() {
        let encoded = match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
            c if c <= '\u{1f}' => 6,
            c => c.len_utf8(),
        };
        if used.saturating_add(encoded) > max_bytes {
            break;
        }
        used += encoded;
        end = index + c.len_utf8();
    }
    (&s[..end], end < s.len())
}

pub fn sanitize_event_data(data: Value, max_bytes: usize) -> (Value, bool) {
    let serialized = serde_json::to_string(&data).expect("JSON values serialize");
    if serialized.len() <= max_bytes {
        return (data, false);
    }
    let mut fields = Map::new();
    fields.insert("original_bytes".into(), json!(serialized.len()));
    fields.insert("summary".into(), json!(""));
    // Preserve bounded association identifiers in an oversized informational event.
    if let Some(object) = data.as_object() {
        for (key, value) in object {
            if (key.ends_with("_id") || key == "messageId" || key == "toolCallId")
                && value.as_str().is_some_and(|s| s.len() <= 256)
            {
                let mut candidate = fields.clone();
                candidate.insert(key.clone(), value.clone());
                if serde_json::to_vec(&candidate).unwrap().len() <= max_bytes {
                    fields = candidate;
                }
            }
        }
    }
    let overhead = serde_json::to_vec(&fields).unwrap().len();
    if overhead > max_bytes {
        return (json!(0), true);
    }
    let (summary, _) = truncate_json_text(&serialized, max_bytes - overhead + 2);
    fields.insert("summary".into(), json!(summary));
    (Value::Object(fields), true)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventPage {
    pub events: Vec<Event>,
    pub earliest_cursor: u64,
    pub next_cursor: u64,
    pub last_cursor: u64,
    pub gap: bool,
    pub dropped_count: u64,
    pub has_more: bool,
}

#[derive(Debug)]
pub struct EventBuffer {
    events: VecDeque<(Event, usize)>,
    next_seq: u64,
    total_bytes: usize,
    keep_count: usize,
    keep_bytes: usize,
}

impl EventBuffer {
    pub fn new(keep_count: usize, keep_bytes: usize) -> Self {
        Self {
            events: VecDeque::new(),
            next_seq: 1,
            total_bytes: 0,
            keep_count,
            keep_bytes,
        }
    }
    pub fn push(&mut self, kind: EventKind, data: Value) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self
            .next_seq
            .checked_add(1)
            .expect("event sequence exhausted");
        let mut event = Event {
            seq,
            time: rfc3339_now(),
            kind,
            data: Value::Null,
            truncated: false,
        };
        let overhead = serde_json::to_vec(&event).unwrap().len() - 4;
        let (data, truncated) =
            sanitize_event_data(data, limits::EVENT_ITEM_MAX.saturating_sub(overhead));
        event.data = data;
        event.truncated = truncated;
        let size = serde_json::to_vec(&event).unwrap().len();
        self.events.push_back((event, size));
        self.total_bytes += size;
        while self.events.len() > self.keep_count || self.total_bytes > self.keep_bytes {
            let (_, size) = self
                .events
                .pop_front()
                .expect("nonempty oversized event buffer");
            self.total_bytes -= size;
        }
        seq
    }
    pub fn last_cursor(&self) -> u64 {
        self.next_seq - 1
    }
    pub fn earliest_cursor(&self) -> u64 {
        self.events
            .front()
            .map(|(event, _)| event.seq)
            .unwrap_or(self.next_seq)
    }
    pub fn page(&self, cursor: u64, limit: usize) -> EventPage {
        let earliest_cursor = self.earliest_cursor();
        let gap = cursor < earliest_cursor.saturating_sub(1);
        let dropped_count = earliest_cursor.saturating_sub(1).saturating_sub(cursor);
        let events: Vec<Event> = self
            .events
            .iter()
            .filter(|(event, _)| event.seq > cursor)
            .take(limit)
            .map(|(event, _)| event.clone())
            .collect();
        let next_cursor = events
            .last()
            .map(|event| event.seq)
            .unwrap_or(cursor.max(earliest_cursor.saturating_sub(1)));
        let has_more = self
            .events
            .back()
            .is_some_and(|(event, _)| event.seq > next_cursor);
        EventPage {
            events,
            earliest_cursor,
            next_cursor,
            last_cursor: self.last_cursor(),
            gap,
            dropped_count,
            has_more,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultSource {
    LastAssistantMessage,
    TurnText,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskResult {
    pub text: String,
    pub truncated: bool,
    pub total_bytes: usize,
    pub source: ResultSource,
}

#[derive(Debug, Default)]
struct BoundedText {
    text: String,
    total_bytes: usize,
    truncated: bool,
}
impl BoundedText {
    fn push(&mut self, text: &str) {
        self.total_bytes = self.total_bytes.saturating_add(text.len());
        // Once a prefix has been cut, later text must not fill its remaining byte gap.
        if self.truncated {
            return;
        }
        let (part, cut) =
            truncate_utf8(text, limits::RESULT_BUDGET.saturating_sub(self.text.len()));
        self.text.push_str(part);
        self.truncated |= cut;
    }
}

#[derive(Debug, Default)]
pub struct ResultAggregator {
    current: BoundedText,
    turn: BoundedText,
    current_id: Option<String>,
    fallback: bool,
}
impl ResultAggregator {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push_chunk(&mut self, text: &str, message_id: Option<&str>) {
        self.turn.push(text);
        match message_id {
            Some(id) => {
                if self.current_id.as_deref() != Some(id) {
                    self.current = BoundedText::default();
                    self.current_id = Some(id.into());
                }
                self.current.push(text);
            }
            None => self.fallback = true,
        }
    }
    pub fn finish(&self) -> TaskResult {
        let empty = self.turn.total_bytes == 0;
        let (text, source) = if empty {
            (&self.turn, ResultSource::Empty)
        } else if self.fallback {
            (&self.turn, ResultSource::TurnText)
        } else {
            (&self.current, ResultSource::LastAssistantMessage)
        };
        let mut result = TaskResult {
            text: String::new(),
            truncated: text.truncated,
            total_bytes: text.total_bytes,
            source,
        };
        let overhead = serde_json::to_vec(&result).unwrap().len() - 2;
        let (prefix, cut) = truncate_json_text(&text.text, limits::RESULT_BUDGET - overhead);
        result.text = prefix.into();
        result.truncated |= cut;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn seq_and_basic_pagination() {
        let mut b = EventBuffer::new(200, 1 << 20);
        for i in 0..5 {
            assert_eq!(b.push(EventKind::AgentMessage, json!({"i": i})), i + 1);
        }
        let p = b.page(0, 3);
        assert_eq!(p.events.len(), 3);
        assert_eq!(p.earliest_cursor, 1);
        assert_eq!(p.next_cursor, 3);
        assert_eq!(p.last_cursor, 5);
        assert!(!p.gap);
        assert_eq!(p.dropped_count, 0);
        assert!(p.has_more);
        let p2 = b.page(3, 100);
        assert_eq!(p2.events.len(), 2);
        assert_eq!(p2.next_cursor, 5);
        assert!(!p2.has_more);
        let p3 = b.page(5, 100);
        assert!(p3.events.is_empty());
        assert_eq!(p3.next_cursor, 5);
        assert!(!p3.has_more);
    }

    #[test]
    fn eviction_by_count_reports_gap() {
        let mut b = EventBuffer::new(3, 1 << 20);
        for i in 0..5 {
            b.push(EventKind::ToolCall, json!({"i": i}));
        }
        assert_eq!(b.earliest_cursor(), 3);
        assert_eq!(b.last_cursor(), 5);
        let p = b.page(0, 100);
        assert!(p.gap);
        assert_eq!(p.dropped_count, 2); // seq 1..=2 已淘汰
        assert_eq!(p.events[0].seq, 3);
        assert_eq!(p.next_cursor, 5);
        assert!(!p.has_more);
    }

    #[test]
    fn eviction_by_bytes() {
        let mut b = EventBuffer::new(200, 64); // 极小的字节上限
        for i in 0..10 {
            b.push(
                EventKind::AgentMessage,
                json!({"payload": "x".repeat(20), "i": i}),
            );
        }
        assert!(b.earliest_cursor() > 1);
        assert!(b.last_cursor() == 10);
    }

    #[test]
    fn empty_buffer_cursor_rules() {
        let b = EventBuffer::new(200, 1 << 20);
        assert_eq!(b.earliest_cursor(), 1); // last_cursor(0)+1
        let p = b.page(0, 50);
        assert!(p.events.is_empty());
        assert_eq!(p.next_cursor, 0);
        assert!(!p.has_more);
        assert!(!p.gap);
    }

    #[test]
    fn truncate_utf8_keeps_valid_boundaries() {
        let (s, t) = truncate_utf8("héllo wörld", 4);
        assert!(std::str::from_utf8(s.as_bytes()).is_ok());
        assert!(t);
        let (s2, t2) = truncate_utf8("abc", 10);
        assert_eq!(s2, "abc");
        assert!(!t2);
        // 多字节字符不能被切半
        let (s3, _) = truncate_utf8("你好世界", 3);
        assert_eq!(s3, "你");
    }

    #[test]
    fn sanitize_oversized_event_data() {
        let big = json!({"text": "y".repeat(9000)});
        let (v, truncated) = sanitize_event_data(big, limits::EVENT_ITEM_MAX);
        assert!(truncated);
        assert!(serde_json::to_string(&v).unwrap().len() <= limits::EVENT_ITEM_MAX);
        assert!(v.get("original_bytes").is_some());
        let (v2, t2) = sanitize_event_data(json!({"a": 1}), limits::EVENT_ITEM_MAX);
        assert!(!t2);
        assert_eq!(v2, json!({"a": 1}));
    }

    #[test]
    fn result_aggregation_by_message_id() {
        let mut a = ResultAggregator::new();
        a.push_chunk("hello ", Some("m1"));
        a.push_chunk("world", Some("m1"));
        a.push_chunk("next msg", Some("m2")); // 新 message 重置
        let r = a.finish();
        assert_eq!(r.text, "next msg");
        assert_eq!(r.source, ResultSource::LastAssistantMessage);
        assert_eq!(r.total_bytes, 8);
        assert!(!r.truncated);
    }

    #[test]
    fn result_aggregation_turn_text_fallback() {
        let mut a = ResultAggregator::new();
        a.push_chunk("part1 ", None);
        a.push_chunk("part2", None);
        let r = a.finish();
        assert_eq!(r.text, "part1 part2");
        assert_eq!(r.source, ResultSource::TurnText);
    }

    #[test]
    fn result_aggregation_empty_and_truncation() {
        let a = ResultAggregator::new();
        let r = a.finish();
        assert_eq!(r.text, "");
        assert_eq!(r.source, ResultSource::Empty);
        let mut b = ResultAggregator::new();
        b.push_chunk(&"z".repeat(70 * 1024), Some("m"));
        let r2 = b.finish();
        assert!(r2.text.len() < limits::RESULT_BUDGET);
        assert!(serde_json::to_vec(&r2).unwrap().len() <= limits::RESULT_BUDGET);
        assert!(r2.truncated);
        assert_eq!(r2.total_bytes, 70 * 1024);
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    #[test]
    fn empty_new_assistant_message_resets_previous_result() {
        let mut result = ResultAggregator::new();
        result.push_chunk("previous message", Some("one"));
        result.push_chunk("", Some("two"));
        let result = result.finish();
        assert_eq!(result.text, "");
        assert_eq!(result.total_bytes, 0);
        assert_eq!(result.source, ResultSource::LastAssistantMessage);
    }
    use serde_json::json;

    #[test]
    fn serialized_events_have_type_and_rfc3339_time() {
        let mut b = EventBuffer::new(200, 1 << 20);
        b.push(
            EventKind::ToolCall,
            json!({"tool_call_id": "tc-1", "text": "\\\"\n".repeat(9000)}),
        );
        let e = b.page(0, 1).events.pop().unwrap();
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["type"], "tool_call");
        assert!(v.get("kind").is_none());
        assert!(e.time.ends_with('Z') && e.time.contains('T'));
        assert!(serde_json::to_vec(&e).unwrap().len() <= limits::EVENT_ITEM_MAX);
        assert_eq!(e.data["tool_call_id"], "tc-1");
        assert!(e.truncated);
    }

    #[test]
    fn sanitizer_counts_json_escaping() {
        let (v, t) = sanitize_event_data(json!({"text": "\\\"\n".repeat(9000)}), 512);
        assert!(t);
        assert!(serde_json::to_vec(&v).unwrap().len() <= 512);
        assert!(v.get("original_bytes").is_some());
    }

    #[test]
    fn events_are_removed_when_one_exceeds_total_budget() {
        let mut b = EventBuffer::new(200, 10);
        b.push(EventKind::BridgeError, json!({"text": "too big"}));
        let p = b.page(0, 50);
        assert!(p.events.is_empty());
        assert_eq!(p.earliest_cursor, 2);
        assert_eq!(p.next_cursor, 1);
        assert_eq!(p.dropped_count, 1);
        assert!(!p.has_more);
    }

    #[test]
    fn mixed_message_ids_fall_back_to_complete_turn_text() {
        let mut a = ResultAggregator::new();
        a.push_chunk("first", Some("m1"));
        a.push_chunk("second", Some("m2"));
        a.push_chunk("unknown", None);
        a.push_chunk("third", Some("m3"));
        let r = a.finish();
        assert_eq!(r.source, ResultSource::TurnText);
        assert_eq!(r.text, "firstsecondunknownthird");
        assert_eq!(r.total_bytes, r.text.len());
    }

    #[test]
    fn result_encoding_budget_is_inclusive_and_unicode_safe() {
        let mut a = ResultAggregator::new();
        a.push_chunk(&"\\\"\n你好".repeat(30000), Some("m"));
        let r = a.finish();
        assert!(serde_json::to_vec(&r).unwrap().len() <= limits::RESULT_BUDGET);
        assert!(r.truncated);
        assert!(r.total_bytes > r.text.len());
    }

    #[test]
    fn utc_conversion_handles_epoch_and_leap_year() {
        assert_eq!(rfc3339_from_unix(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_from_unix(951782400), "2000-02-29T00:00:00Z");
    }
}
