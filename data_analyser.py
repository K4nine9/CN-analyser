import pandas as pd
import sys
import os

def analyze_data(file_path):
    print(f"Loading data from {file_path}...")
    try:
        # Load the data, assuming TSV format
        df = pd.read_csv(file_path, sep='\t', on_bad_lines='skip', low_memory=False)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    report_lines = []
    report_lines.append(f"Analysis Report for {file_path}")
    report_lines.append(f"Total Rows: {len(df)}")
    report_lines.append(f"Total Columns: {len(df.columns)}")
    report_lines.append("-" * 30)

    print(f"Data loaded. Rows: {len(df)}, Columns: {len(df.columns)}")

    for col in df.columns:
        print(f"Analyzing column: {col}")
        report_lines.append(f"\nColumn: {col}")
        report_lines.append(f"Type: {df[col].dtype}")

        # Basic counts
        total_count = len(df)
        non_null_count = df[col].count()
        null_count = total_count - non_null_count
        unique_count = df[col].nunique()

        report_lines.append(f"Non-Null Count: {non_null_count}")
        report_lines.append(f"Null Count: {null_count}")
        report_lines.append(f"Unique Values: {unique_count}")

        # Unique value analysis (for all columns, but limited output)
        if unique_count > 0:
            report_lines.append("Top 20 Frequent Values:")
            value_counts = df[col].value_counts().head(20)
            for val, count in value_counts.items():
                percentage = (count / total_count) * 100
                report_lines.append(f"  {val}: {count} ({percentage:.2f}%)")
            
            if unique_count > 20:
                report_lines.append(f"  ... and {unique_count - 20} more unique values.")

        # Numerical statistics
        if pd.api.types.is_numeric_dtype(df[col]):
            stats = df[col].describe()
            report_lines.append("Numerical Statistics:")
            for stat, value in stats.items():
                report_lines.append(f"  {stat}: {value}")

        report_lines.append("-" * 30)

    # Save report
    output_file = "analysis_report.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    
    print(f"Analysis complete. Report saved to {output_file}")

if __name__ == "__main__":
    target_file = r"datas/notes-00000.tsv"
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    
    if os.path.exists(target_file):
        analyze_data(target_file)
    else:
        # Fallback to absolute path if running from root but file is in datas
        abs_path = os.path.join(os.getcwd(), target_file)
        if os.path.exists(abs_path):
            analyze_data(abs_path)
        else:
            print(f"File not found: {target_file}")
