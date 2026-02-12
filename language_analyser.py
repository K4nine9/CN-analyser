import pandas as pd
from langdetect import detect, DetectorFactory, LangDetectException
from tqdm import tqdm
import sys
import os
import argparse

# Ensure consistent results
DetectorFactory.seed = 0

def detect_language(text):
    if not isinstance(text, str) or not text.strip():
        return "unknown"
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"
    except Exception:
        return "error"

def analyze_language(file_path, limit=None):
    print(f"Loading data from {file_path} (summary column only)...")
    try:
        # Load only necessary columns to save memory
        df = pd.read_csv(file_path, sep='\t', usecols=['noteId', 'summary'], on_bad_lines='skip', low_memory=False, nrows=limit)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    print(f"Data loaded. Rows: {len(df)}")
    
    # Register tqdm with pandas
    tqdm.pandas(desc="Detecting Languages")
    
    # Apply language detection
    print("Starting language detection (this may take a while)...")
    df['language'] = df['summary'].progress_apply(detect_language)
    
    # Calculate stats
    lang_counts = df['language'].value_counts()
    total_count = len(df)
    
    report_lines = []
    report_lines.append(f"Language Analysis Report for {file_path}")
    report_lines.append(f"Total Rows: {total_count}")
    report_lines.append("-" * 30)
    report_lines.append("Language Distribution:")
    
    for lang, count in lang_counts.items():
        percentage = (count / total_count) * 100
        report_lines.append(f"  {lang}: {count} ({percentage:.2f}%)")
        
    # Save report
    report_file = "analysis_report_language.txt"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    
    print(f"Language analysis complete. Report saved to {report_file}")
    
    # Save detailed results
    output_tsv = "note_languages.tsv"
    print(f"Saving per-note language data to {output_tsv}...")
    df[['noteId', 'language']].to_csv(output_tsv, sep='\t', index=False)
    print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze language of notes.")
    parser.add_argument("file_path", nargs="?", default="datas/notes-00000.tsv", help="Path to TSV file")
    parser.add_argument("--limit", type=int, help="Limit number of rows to process")
    args = parser.parse_args()

    target_file = args.file_path
    if not os.path.exists(target_file):
         # Fallback to absolute path
        abs_path = os.path.join(os.getcwd(), target_file)
        if os.path.exists(abs_path):
            target_file = abs_path
        else:
            print(f"File not found: {target_file}")
            sys.exit(1)

    analyze_language(target_file, limit=args.limit)
