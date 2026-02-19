
import sqlite3 as sq3
import pandas as pd
import numpy as np
import os
import sys
import datetime as dt
from langdetect import detect, DetectorFactory, LangDetectException
from tqdm import tqdm
import multiprocessing as mp

# Increase CSV field size limit for large fields
import csv
csv.field_size_limit(sys.maxsize)

# Ensure consistent results for langdetect
DetectorFactory.seed = 0

# --- Worker Function for Parallel Processing ---
def detect_language_worker(text):
    if not isinstance(text, str) or not text.strip():
        return "unknown"
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"
    except Exception:
        return "error"

def process_language_chunk(texts):
    return [detect_language_worker(t) for t in texts]

# --- DB Creation (Exact copy of original schema) ---
def Make_DB() -> str:
    print("Creating DB...")

    db_path = f"note_data_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite3"
    conn = sq3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE note_data (
        noteId INTEGER PRIMARY KEY,
        noteAuthorParticipantId TEXT,
        createdAtMillis INTEGER,
        tweetId INTEGER,
        classification TEXT,
        believable TEXT,
        harmful TEXT,
        validationDifficulty TEXT,
        misleadingOther INTEGER,
        misleadingFactualError INTEGER,
        misleadingManipulatedMedia INTEGER,
        misleadingOutdatedInformation INTEGER,
        misleadingMissingImportantContext INTEGER,
        misleadingUnverifiedClaimAsFact INTEGER,
        misleadingSatire INTEGER,
        notMisleadingOther INTEGER,
        notMisleadingFactuallyCorrect INTEGER,
        notMisleadingOutdatedButNotWhenWritten INTEGER,
        notMisleadingClearlySatire INTEGER,
        notMisleadingPersonalOpinion INTEGER,
        trustworthySources INTEGER,
        summary TEXT,
        isMediaNote INTEGER,
        isCollaborativeNote INTEGER,
        note_language TEXT
    )
    """)
    conn.commit()

    cursor.execute("""
    CREATE TABLE note_status_history (
        noteId INTEGER,
        participantId TEXT,
        createdAtMillis INTEGER,
        timestampMillisOfFirstNonNMRStatus INTEGER,
        firstNonNMRStatus TEXT,
        timestampMillisOfCurrentStatus INTEGER,
        currentStatus TEXT,
        timestampMillisOfLatestNonNMRStatus INTEGER,
        latestNonNMRStatus TEXT,
        timestampMillisOfStatusLock INTEGER,
        lockedStatus TEXT,
        timestampMillisOfRetroLock INTEGER,
        currentCoreStatus TEXT,
        currentExpansionStatus TEXT,
        currentGroupStatus TEXT,
        currentDecidedByKey TEXT,
        currentModelingGroup INTEGER,
        timestampMillisOfMostRecentStatusChange INTEGER,
        timestampMillisOfNmrDueToMinStableCrhTime INTEGER,
        currentMultiGroupStatus TEXT,
        currentModelingMultiGroup INTEGER,
        timestampMinuteOfFinalScoringOutput TEXT,
        timestampMillisOfFirstNmrDueToMinStableCrhTime INTEGER,
        FOREIGN KEY (noteId) REFERENCES note_data(noteId)
    )
    """)
    conn.commit()

    cursor.execute("""
    CREATE TABLE ratings (
        noteId INTEGER,
        participantId TEXT,
        createdAtMillis INTEGER,
        ratedOnTweetId INTEGER,
        ratingSourceBucketed TEXT,
        suggestion TEXT,
        helpfulnessLevel TEXT,
        helpfulOther INTEGER,
        helpfulInformative INTEGER,
        helpfulClear INTEGER,
        helpfulEmpathetic INTEGER,
        helpfulGoodSources INTEGER,
        helpfulUniqueContext INTEGER,
        helpfulAddressesClaim INTEGER,
        helpfulImportantContext INTEGER,
        helpfulUnbiasedLanguage INTEGER,
        notHelpfulOther INTEGER,
        notHelpfulIncorrect INTEGER,
        notHelpfulSourcesMissingOrUnreliable INTEGER,
        notHelpfulOpinionSpeculationOrBias INTEGER,
        notHelpfulMissingKeyPoints INTEGER,
        notHelpfulOutdated INTEGER,
        notHelpfulHardToUnderstand INTEGER,
        notHelpfulArgumentativeOrBiased INTEGER,
        notHelpfulOffTopic INTEGER,
        notHelpfulSpamHarassmentOrAbuse INTEGER,
        notHelpfulIrrelevantSources INTEGER,
        notHelpfulOpinionSpeculation INTEGER,
        notHelpfulNoteNotNeeded INTEGER,
        PRIMARY KEY (noteId, participantId),
        FOREIGN KEY (noteId) REFERENCES note_data(noteId)
    )
    """)
    conn.commit()

    cursor.execute("""
    CREATE TABLE user_enrollment (
        participantId TEXT PRIMARY KEY,
        enrollmentState TEXT,
        successfulRatingNeededToEarnIn INTEGER,
        timestampOfLastStateChange INTEGER,
        timestampOfLastEarnOut INTEGER,
        modelingPopulation TEXT,
        modelingGroup INTEGER
    )
    """)
    conn.commit()

    cursor.execute("""
    CREATE TABLE bat_signals (
        tweetId INTEGER PRIMARY KEY,
        sourceLinks TEXT,
        noteRequestFeedEligibleTimestamp INTEGER,
        apiSmallFeedEligibleTimestamp INTEGER,
        apiLargeFeedEligibleTimestamp INTEGER
    )
    """)
    conn.commit()

    conn.close()
    print("DB created")
    return db_path

# --- Helpers ---

def clean_int(series, default=0):
    return pd.to_numeric(series, errors='coerce').fillna(default).astype(int)

def clean_str(series, default=""):
    return series.fillna(default).astype(str)

# --- Processors ---

def process_notes(file_path, db_path, chunk_size=20000):
    print(f"Processing Notes Data: {file_path}")
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    # Columns mapping based on NoteData class
    # noteId, noteAuthorParticipantId, createdAtMillis, tweetId, classification,
    # believable, harmful, validationDifficulty, misleadingOther, misleadingFactualError,
    # misleadingManipulatedMedia, misleadingOutdatedInformation, misleadingMissingImportantContext,
    # misleadingUnverifiedClaimAsFact, misleadingSatire, notMisleadingOther, notMisleadingFactuallyCorrect,
    # notMisleadingOutdatedButNotWhenWritten, notMisleadingClearlySatire, notMisleadingPersonalOpinion,
    # trustworthySources, summary, isMediaNote, isCollaborativeNote
    
    # We read as strings to handle nulls safely before conversion
    for chunk in tqdm(pd.read_csv(file_path, sep='\t', chunksize=chunk_size, dtype=str, on_bad_lines='skip', quoting=csv.QUOTE_MINIMAL), desc="Notes chunks"):
        try:
            df = chunk.copy()
            
            # Map columns to match schema order and types
            # Integers (default -1)
            df['noteId'] = clean_int(df.get('noteId'), -1)
            df['tweetId'] = clean_int(df.get('tweetId'), -1)
            
            # Integers (default 0)
            int_zero_cols = [
                'createdAtMillis', 'misleadingOther', 'misleadingFactualError', 'misleadingManipulatedMedia',
                'misleadingOutdatedInformation', 'misleadingMissingImportantContext', 'misleadingUnverifiedClaimAsFact',
                'misleadingSatire', 'notMisleadingOther', 'notMisleadingFactuallyCorrect', 
                'notMisleadingOutdatedButNotWhenWritten', 'notMisleadingClearlySatire', 'notMisleadingPersonalOpinion',
                'trustworthySources', 'isMediaNote', 'isCollaborativeNote'
            ]
            for col in int_zero_cols:
                df[col] = clean_int(df.get(col), 0)
                
            # Strings
            str_cols = [
                'noteAuthorParticipantId', 'classification', 'believable', 'harmful', 'validationDifficulty', 'summary'
            ]
            for col in str_cols:
                df[col] = clean_str(df.get(col), "")
                
            # Language Detection (Parallel)
            # Only parallelize if we have enough rows to justify overhead
            texts = df['summary'].tolist()
            if len(texts) > 100:
                with mp.Pool(processes=min(mp.cpu_count(), 8)) as pool:
                    # Chunk the texts for workers to reduce IPC overhead
                    # Or just pool.map which chunks automatically
                     df['note_language'] = pool.map(detect_language_worker, texts)
            else:
                df['note_language'] = [detect_language_worker(t) for t in texts]

            # Prepare list of tuples
            # Column order must match CREATE TABLE and INSERT
            cols_ordered = [
                'noteId', 'noteAuthorParticipantId', 'createdAtMillis', 'tweetId', 
                'classification', 'believable', 'harmful', 'validationDifficulty',
                'misleadingOther', 'misleadingFactualError', 'misleadingManipulatedMedia',
                'misleadingOutdatedInformation', 'misleadingMissingImportantContext',
                'misleadingUnverifiedClaimAsFact', 'misleadingSatire',
                'notMisleadingOther', 'notMisleadingFactuallyCorrect',
                'notMisleadingOutdatedButNotWhenWritten', 'notMisleadingClearlySatire',
                'notMisleadingPersonalOpinion', 'trustworthySources', 'summary',
                'isMediaNote', 'isCollaborativeNote', 'note_language'
            ]
            
            data_to_insert = list(df[cols_ordered].itertuples(index=False, name=None))
            
            cursor.executemany("""
            INSERT OR IGNORE INTO note_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, data_to_insert)
            conn.commit()
            
        except Exception as e:
            print(f"Error processing chunk: {e}")
            continue

    conn.close()

def make_db_report(db_path: str, path_list: list) -> None:
    with open(f"report_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "w", encoding="utf-8") as f:
        f.write(f"DB Path: {db_path}\n")
        f.write(f"Path List: {path_list}\n")

def main() -> None:
    # On Windows, multiprocessing needs freeze_support if bundled, 
    # but for script execution, standard guard is enough.
    mp.freeze_support() 
    
    db_path = Make_DB()

    notes_path_list = []
    # logic to find files
    for root, dirs, files in os.walk("datas"):
        for file in files:
            if file.startswith("notes-") and file.endswith(".tsv"):
                notes_path_list.append(os.path.join(root, file))
    
    print("Processing notes data...")
    for path in notes_path_list:
        process_notes(path, db_path)
    
    # Placeholder for other tables (commented out in original)
    # If implementing fully, add functions for process_ratings etc.
    
    make_db_report(db_path, notes_path_list)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
