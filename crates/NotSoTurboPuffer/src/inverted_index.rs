/// A data structure that maps fields to their values and the documents that contain those values.
///
/// Field -> Value (Stringified) -> List of `DocIDs`
/// This makes it easy to search for documents that contain a specific value for a specific field and makes attribute based filtering efficient.
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::engine::{AttributeValue, DocumentId, Row};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvertedIndex {
    /// Field -> Value (Stringified) -> List of `DocIDs`
    pub index: HashMap<String, HashMap<String, Vec<DocumentId>>>,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    pub fn build_from_rows(rows: &[Row], attributes: &HashSet<String>) -> Self {
        let mut index = Self::new();
        index.insert_rows(rows, attributes);
        index
    }

    /// Inserts entries for the rows. An empty `attributes` set indexes every
    /// attribute. Posting lists stay sorted and deduplicated.
    pub fn insert_rows(&mut self, rows: &[Row], attributes: &HashSet<String>) {
        let index_all = attributes.is_empty();

        for row in rows {
            for (key, value) in row.attributes.iter() {
                if !index_all && !attributes.contains(key) {
                    continue;
                }

                let value_key = Self::canonicalize_value(value);
                self.index
                    .entry(key.clone())
                    .or_default()
                    .entry(value_key)
                    .or_default()
                    .push(row.id.clone());
            }
        }

        // Sort for deterministic output / potentially faster intersection (if we used sorted arrays)
        for field_map in self.index.values_mut() {
            for doc_list in field_map.values_mut() {
                doc_list.sort();
                doc_list.dedup();
            }
        }
    }

    /// Removes the documents from every posting list. Callers apply a
    /// compaction batch as remove-then-insert: remove every touched document
    /// (updated or deleted), then insert the surviving rows. Documents from
    /// earlier compactions keep their entries.
    pub fn remove_documents(&mut self, ids: &HashSet<DocumentId>) {
        if ids.is_empty() {
            return;
        }

        for field_map in self.index.values_mut() {
            for doc_list in field_map.values_mut() {
                doc_list.retain(|id| !ids.contains(id));
            }
            field_map.retain(|_, doc_list| !doc_list.is_empty());
        }
        self.index.retain(|_, field_map| !field_map.is_empty());
    }

    /// Returns the set of Document IDs that match ALL filters.
    /// Filters are Field -> Value.
    pub fn filter(&self, filters: &HashMap<String, String>) -> Option<HashSet<DocumentId>> {
        if filters.is_empty() {
            return None;
        }

        let mut result_set: Option<HashSet<DocumentId>> = None;

        for (field, value) in filters {
            let Some(field_index) = self.index.get(field) else {
                return Some(HashSet::new());
            };

            let Some(matches) = field_index.get(value) else {
                return Some(HashSet::new()); // Value not found -> No matches
            };

            match &mut result_set {
                None => {
                    // First filter, initialize set
                    result_set = Some(matches.iter().cloned().collect());
                }
                Some(current_set) => {
                    // Intersection
                    // Efficient intersection: retain only those in `matches`
                    let match_set: HashSet<&DocumentId> = matches.iter().collect();
                    current_set.retain(|id| match_set.contains(id));

                    if current_set.is_empty() {
                        return Some(HashSet::new());
                    }
                }
            }
        }

        result_set
    }

    // Helper to canonicalize AttributeValue or JSON Value for index keys
    pub fn canonicalize_value(value: &AttributeValue) -> String {
        match value {
            AttributeValue::String(s) => s.clone(),
            AttributeValue::Int(i) => i.to_string(),
            AttributeValue::Uint(u) => u.to_string(),
            AttributeValue::Float(f) => f.to_string(),
            AttributeValue::Bool(b) => b.to_string(),
            AttributeValue::Uuid(u) => u.to_string(),
            AttributeValue::DateTime(t) => t.to_string(),
            AttributeValue::Null => "null".to_string(),
            // For arrays, we might want to index each element?
            // Current implementation: Treat array as a string (exact match of array)
            // Ideally we'd flatten arrays (e.g. tag IN tags).
            // For complex types, use debug format
            _ => format!("{value:?}"),
        }
    }

    pub fn canonicalize_json_value(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "null".to_string(),
            _ => value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DocumentId;

    #[test]
    fn test_inverted_index_filtering() {
        let mut rows = Vec::new();

        let mut attrs1 = HashMap::new();
        attrs1.insert("tag".to_string(), AttributeValue::String("A".to_string()));
        attrs1.insert("num".to_string(), AttributeValue::Int(10));
        rows.push(Row {
            id: DocumentId::from("doc1"),
            vector: vec![],
            attributes: attrs1,
            timestamp: 0,
        });

        let mut attrs2 = HashMap::new();
        attrs2.insert("tag".to_string(), AttributeValue::String("B".to_string()));
        attrs2.insert("num".to_string(), AttributeValue::Int(10));
        rows.push(Row {
            id: DocumentId::from("doc2"),
            vector: vec![],
            attributes: attrs2,
            timestamp: 0,
        });

        let mut attrs3 = HashMap::new();
        attrs3.insert("tag".to_string(), AttributeValue::String("A".to_string()));
        attrs3.insert("num".to_string(), AttributeValue::Int(20));
        rows.push(Row {
            id: DocumentId::from("doc3"),
            vector: vec![],
            attributes: attrs3,
            timestamp: 0,
        });

        let index = InvertedIndex::build_from_rows(&rows, &HashSet::new());

        // Test Filter: tag=A
        let mut filter = HashMap::new();
        filter.insert("tag".to_string(), "A".to_string());
        let results = index.filter(&filter).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&DocumentId::from("doc1")));
        assert!(results.contains(&DocumentId::from("doc3")));

        // Test Filter: tag=A AND num=10
        filter.insert("num".to_string(), "10".to_string());
        let results = index.filter(&filter).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&DocumentId::from("doc1")));

        // Test Filter: num=99 (No match)
        let mut filter_none = HashMap::new();
        filter_none.insert("num".to_string(), "99".to_string());
        let results = index.filter(&filter_none).unwrap();
        assert!(results.is_empty());
    }

    fn row(id: &str, tag: &str) -> Row {
        let mut attrs = HashMap::new();
        attrs.insert("tag".to_string(), AttributeValue::String(tag.to_string()));
        Row {
            id: DocumentId::from(id),
            vector: vec![],
            attributes: attrs,
            timestamp: 0,
        }
    }

    fn filter_ids(index: &InvertedIndex, field: &str, value: &str) -> HashSet<DocumentId> {
        let mut filter = HashMap::new();
        filter.insert(field.to_string(), value.to_string());
        index.filter(&filter).unwrap()
    }

    #[test]
    fn test_compaction_batch_merge_keeps_untouched_documents() {
        // Cycle 1: index doc1 (tag=A) and doc2 (tag=B).
        let mut index =
            InvertedIndex::build_from_rows(&[row("doc1", "A"), row("doc2", "B")], &HashSet::new());

        // Cycle 2: doc2 updates to tag=A, doc3 arrives (tag=B), doc1 untouched.
        let batch = [row("doc2", "A"), row("doc3", "B")];
        let touched: HashSet<DocumentId> = batch.iter().map(|r| r.id.clone()).collect();
        index.remove_documents(&touched);
        index.insert_rows(&batch, &HashSet::new());

        // doc1 keeps its entry from cycle 1; doc2 moved from B to A.
        let tag_a = filter_ids(&index, "tag", "A");
        assert_eq!(tag_a.len(), 2);
        assert!(tag_a.contains(&DocumentId::from("doc1")));
        assert!(tag_a.contains(&DocumentId::from("doc2")));

        let tag_b = filter_ids(&index, "tag", "B");
        assert_eq!(tag_b.len(), 1);
        assert!(tag_b.contains(&DocumentId::from("doc3")));

        // Cycle 3: doc1 deleted (tombstone only, no surviving row).
        let deleted: HashSet<DocumentId> = [DocumentId::from("doc1")].into();
        index.remove_documents(&deleted);
        index.insert_rows(&[], &HashSet::new());

        let tag_a = filter_ids(&index, "tag", "A");
        assert_eq!(tag_a.len(), 1);
        assert!(tag_a.contains(&DocumentId::from("doc2")));
    }
}
