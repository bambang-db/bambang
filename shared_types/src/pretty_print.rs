use crate::row::Row;
use crate::schema::Schema;
use crate::value::Value;
use std::fmt::Write;

pub fn pretty_print_rows(rows: &Vec<Row>, schema: &Schema) -> String {
    if rows.is_empty() {
        return "No rows to display".to_string();
    }

    let mut output = String::new();
    let max_columns = schema.column_count().max(rows.iter().map(|r| r.column_count()).max().unwrap_or(0));
    
    let mut column_widths = vec![0; max_columns + 1];
    column_widths[0] = "ID".len();
    
    for i in 0..max_columns {
        let column_name = if i < schema.columns.len() {
            &schema.columns[i].name
        } else {
            "Unknown"
        };
        column_widths[i + 1] = column_name.len();
    }
    
    for row in rows {
        column_widths[0] = column_widths[0].max(row.id.to_string().len());
        for (i, value) in row.data.iter().enumerate() {
            let value_str = format_value(value);
            column_widths[i + 1] = column_widths[i + 1].max(value_str.len());
        }
    }
    
    write!(&mut output, "┌{}", "─".repeat(column_widths[0] + 2)).unwrap();
    for i in 1..=max_columns {
        write!(&mut output, "┬{}", "─".repeat(column_widths[i] + 2)).unwrap();
    }
    writeln!(&mut output, "┐").unwrap();
    
    write!(&mut output, "│ {:width$} ", "ID", width = column_widths[0]).unwrap();
    for i in 0..max_columns {
        let column_name = if i < schema.columns.len() {
            &schema.columns[i].name
        } else {
            "Unknown"
        };
        write!(&mut output, "│ {:width$} ", column_name, width = column_widths[i + 1]).unwrap();
    }
    writeln!(&mut output, "│").unwrap();
    
    write!(&mut output, "├{}", "─".repeat(column_widths[0] + 2)).unwrap();
    for i in 0..max_columns {
        write!(&mut output, "┼{}", "─".repeat(column_widths[i + 1] + 2)).unwrap();
    }
    writeln!(&mut output, "┤").unwrap();
    
    for row in rows {
        write!(&mut output, "│ {:width$} ", row.id, width = column_widths[0]).unwrap();
        for i in 0..max_columns {
            let value_str = if i < row.data.len() {
                format_value(&row.data[i])
            } else {
                "NULL".to_string()
            };
            write!(&mut output, "│ {:width$} ", value_str, width = column_widths[i + 1]).unwrap();
        }
        writeln!(&mut output, "│").unwrap();
    }
    
    write!(&mut output, "└{}", "─".repeat(column_widths[0] + 2)).unwrap();
    for i in 0..max_columns {
        write!(&mut output, "┴{}", "─".repeat(column_widths[i + 1] + 2)).unwrap();
    }
    writeln!(&mut output, "┘").unwrap();
    
    output
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Integer(i) => i.to_string(),
        Value::String(s) => s.clone(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Decimal(d) => d.clone(),
        Value::Binary(b) => format!("Binary({} bytes)", b.len()),
        Value::Date(d) => format!("Date({})", d),
        Value::Time(t) => format!("Time({})", t),
        Value::Timestamp(ts) => format!("Timestamp({})", ts),
        Value::DateTime(dt) => format!("DateTime({})", dt),
        Value::Json(j) => j.clone(),
        Value::Uuid(u) => format!("UUID({:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x})",
            u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]),
        Value::Text(t) => t.clone(),
        Value::Char(c) => c.to_string(),
        Value::TinyInt(i) => i.to_string(),
    }
}