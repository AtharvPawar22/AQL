use clap::Parser;
use colored::*;
use std::collections::HashMap;
use std::error::Error;

#[derive(Debug)]
struct Query {
    table_name: String,
    filter: Option<Filter>,
    columns: Option<Vec<String>>,
    sort_column: Option<String>,
    sort_desc: bool,
    limit: Option<usize>,
}

#[derive(Debug)]
struct Filter {
    column: String,
    operator: String,
    value: String,
}

#[derive(Parser)]
#[command(name = "flexiql")]
#[command(about = "A simple CSV query language")]
struct Cli {
    query: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let query = parse_query(&cli.query)?;

    let results = execute_query(query)?;

    print_results(results);

    Ok(())
}

fn parse_query(input: &str) -> Result<Query, String> {
    let parts: Vec<&str> = input.split(">>").map(|s| s.trim()).collect();

    if parts.is_empty() {
        return Err("Empty query".to_string());
    }

    // First part is always the table name
    let table_name = parts[0].to_string();

    let mut query = Query {
        table_name: format!("{}.csv", table_name),
        filter: None,
        columns: None,
        sort_column: None,
        sort_desc: false,
        limit: None,
    };

    for part in &parts[1..] {
        let words: Vec<&str> = part.split_whitespace().collect();

        if words.is_empty() {
            continue;
        }

        match words[0].to_lowercase().as_str() {
            "show" => {
                let columns_str = part.strip_prefix("show").unwrap().trim();
                let columns: Vec<String> = columns_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                query.columns = Some(columns);
            }
            "sort" => {
                if words.len() >= 2 {
                    query.sort_column = Some(words[1].to_string());
                    if words.len() >= 3 && words[2].to_lowercase() == "desc" {
                        query.sort_desc = true;
                    }
                }
            }
            "take" | "limit" => {
                if words.len() >= 2 {
                    query.limit = words[1].parse().ok();
                }
            }
            _ => {
                query.filter = parse_filter(part)?;
            }
        }
    }

    Ok(query)
}

fn parse_filter(filter_str: &str) -> Result<Option<Filter>, String> {
    let words: Vec<&str> = filter_str.split_whitespace().collect();

    if words.len() < 3 {
        return Err(format!("Invalid filter: {}", filter_str));
    }

    let column = words[0].to_string();
    let (operator, value_start_index) = {
        if words.len() >= 4 && words[1] == "greater" && words[2] == "than" {
            ("greater".to_string(), 3)
        } else if words.len() >= 4 && words[1] == "less" && words[2] == "than" {
            ("less".to_string(), 3)
        } else if words[1] == "equals" {
            ("equals".to_string(), 2)
        } else if words[1] == "contains" {
            ("contains".to_string(), 2)
        } else {
            (words[1].to_string(), 2)
        }
    };

    if words.len() <= value_start_index {
        return Err(format!("Missing value in filter: {}", filter_str));
    }
    let value = words[value_start_index..].join(" ");

    Ok(Some(Filter {
        column,
        operator,
        value,
    }))
}

fn execute_query(query: Query) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let mut reader = csv::Reader::from_path(&query.table_name)?;
    let headers = reader.headers()?.clone();
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    let mut header_map = HashMap::new();
    for (i, header) in header_names.iter().enumerate() {
        header_map.insert(header.clone(), i);
    }

    let mut rows: Vec<Vec<String>> = Vec::new();
    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        rows.push(row);
    }

    if let Some(filter) = &query.filter {
        rows = apply_filter(rows, filter, &header_map)?;
    }

    if let Some(sort_col) = &query.sort_column {
        apply_sort(&mut rows, sort_col, query.sort_desc, &header_map)?;
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
    let final_rows = if let Some(columns) = &query.columns {
        select_columns(rows, columns, &header_names, &header_map)?
    } else {
        let mut result = vec![header_names];
        result.extend(rows);
        result
    };

    Ok(final_rows)
}

fn apply_filter(
    rows: Vec<Vec<String>>,
    filter: &Filter,
    header_map: &HashMap<String, usize>,
) -> Result<Vec<Vec<String>>, String> {
    let column_index = header_map
        .get(&filter.column)
        .ok_or_else(|| format!("Column '{}' not found", filter.column))?;

    let filtered_rows: Vec<Vec<String>> = rows
        .into_iter()
        .filter(|row| {
            if let Some(cell_value) = row.get(*column_index) {
                check_condition(cell_value, &filter.operator, &filter.value)
            } else {
                false
            }
        })
        .collect();

    Ok(filtered_rows)
}

fn check_condition(cell_value: &str, operator: &str, filter_value: &str) -> bool {
    match operator {
        "equals" | "=" | "==" => cell_value.to_lowercase() == filter_value.to_lowercase(),
        "greater" | ">" => match (cell_value.parse::<f64>(), filter_value.parse::<f64>()) {
            (Ok(a), Ok(b)) => a > b,
            _ => cell_value > filter_value,
        },
        "less" | "<" => match (cell_value.parse::<f64>(), filter_value.parse::<f64>()) {
            (Ok(a), Ok(b)) => a < b,
            _ => cell_value < filter_value,
        },
        "contains" => cell_value
            .to_lowercase()
            .contains(&filter_value.to_lowercase()),
        _ => false,
    }
}

fn apply_sort(
    rows: &mut Vec<Vec<String>>,
    sort_column: &str,
    descending: bool,
    header_map: &HashMap<String, usize>,
) -> Result<(), String> {
    let column_index = header_map
        .get(sort_column)
        .ok_or_else(|| format!("Column '{}' not found", sort_column))?;

    let empty_string = String::new();

    rows.sort_by(|a, b| {
        let val_a = a.get(*column_index).unwrap_or(&empty_string);
        let val_b = b.get(*column_index).unwrap_or(&empty_string);

        let comparison = match (val_a.parse::<f64>(), val_b.parse::<f64>()) {
            (Ok(num_a), Ok(num_b)) => num_a.partial_cmp(&num_b).unwrap(),
            _ => val_a.cmp(val_b),
        };

        if descending {
            comparison.reverse()
        } else {
            comparison
        }
    });

    Ok(())
}

fn select_columns(
    rows: Vec<Vec<String>>,
    columns: &[String],
    _header_names: &[String],
    header_map: &HashMap<String, usize>,
) -> Result<Vec<Vec<String>>, String> {
    let mut column_indices = Vec::new();
    for col in columns {
        let index = header_map
            .get(col)
            .ok_or_else(|| format!("Column '{}' not found", col))?;
        column_indices.push(*index);
    }

    let mut result = Vec::new();

    result.push(columns.to_vec());
    for row in rows {
        let selected_row: Vec<String> = column_indices
            .iter()
            .map(|&i| row.get(i).unwrap_or(&String::new()).to_string())
            .collect();
        result.push(selected_row);
    }

    Ok(result)
}

fn print_results(results: Vec<Vec<String>>) {
    if results.is_empty() {
        println!("{}", "No results found.".yellow());
        return;
    }

    let mut col_widths = vec![0; results[0].len()];
    for row in &results {
        for (i, cell) in row.iter().enumerate() {
            if cell.len() > col_widths[i] {
                col_widths[i] = cell.len();
            }
        }
    }

    // Print table
    for (row_index, row) in results.iter().enumerate() {
        let row_str: String = row
            .iter()
            .enumerate()
            .map(|(i, cell)| format!("{:<width$}", cell, width = col_widths[i]))
            .collect::<Vec<_>>()
            .join(" | ");

        if row_index == 0 {
            println!("{}", row_str.cyan().bold());

            let separator: String = col_widths
                .iter()
                .map(|w| "-".repeat(*w))
                .collect::<Vec<_>>()
                .join("-|-");
            println!("{}", separator.cyan());
        } else {
            println!("{}", row_str);
        }
    }

    println!("\n{}", format!("({} rows)", results.len() - 1).dimmed());
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_parse_simple_query() {
//         let query = parse_query("employees >> show name").unwrap();
//         assert_eq!(query.table_name, "employees.csv");
//         assert!(query.columns.is_some());
//     }

//     #[test]
//     fn test_parse_filter_query() {
//         let query =
//             parse_query("employees >> salary greater than 50000 >> show name, salary").unwrap();
//         assert!(query.filter.is_some());
//         assert!(query.columns.is_some());
//     }
// }
