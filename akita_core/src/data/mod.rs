/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *  
 */

mod row;
mod rows;
mod column_index;

pub use row::*;
pub use rows::*;
pub use column_index::*;



#[cfg(test)]
mod tests {
    use crate::AkitaValue;
    use super::*;

    #[test]
    fn test_rows_creation() {
        let rows = Rows::new();
        assert!(rows.is_empty());
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_row_operations() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let data = vec![AkitaValue::Int(1), AkitaValue::Text("test".to_string())];
        let row = Row::new(columns, data);

        assert_eq!(row.len(), 2);
        assert!(row.contains_column("id"));
        assert!(!row.contains_column("nonexistent"));

        assert_eq!(row.get::<i32, _>(0), Some(1));
        assert_eq!(row.get_by_column::<String>("name"), Some("test".to_string()));
    }

    #[test]
    fn test_rows_iteration() {
        let mut rows = Rows::new();

        let row1 = Row::new(
            vec!["id".to_string(), "name".to_string()],
            vec![AkitaValue::Int(1), AkitaValue::Text("Alice".to_string())],
        );

        let row2 = Row::new(
            vec!["id".to_string(), "name".to_string()],
            vec![AkitaValue::Int(2), AkitaValue::Text("Bob".to_string())],
        );

        rows.push(row1);
        rows.push(row2);

        let mut count = 0;
        for row in &rows {
            assert_eq!(row.len(), 2);
            count += 1;
        }
        assert_eq!(count, 2);

        // 测试对象迭代器
        let objects: Vec<AkitaValue> = rows.object_iter().collect();
        assert_eq!(objects.len(), 2);
    }
}