import sqlite3 as sq3
import os
from typing import List, Tuple
from tqdm.contrib import tenumerate

from CNcore import NoteData

def Make_DB() -> None:
    if os.path.exists("note_data.sqlite3"):
        return
    print("DB not found, creating...")

    # DBを作成
    conn = sq3.connect("note_data.sqlite3")
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE note_data (
        note_id INTEGER PRIMARY KEY,
        note_author_participant_id INTEGER,
        created_at_millis INTEGER,
        tweet_id INTEGER,
        classification INTEGER,
        believable INTEGER,
        harmful INTEGER,
        validation_difficulty INTEGER,
        misleading_other INTEGER,
        misleading_factual_error INTEGER,
        misleading_manipulated_media INTEGER,
        misleading_outdated_information INTEGER,
        misleading_missing_important_context INTEGER,
        misleading_unverified_claim_as_fact INTEGER,
        misleading_satire INTEGER,
        not_misleading_other INTEGER,
        not_misleading_factually_correct INTEGER,
        not_misleading_outdated_but_not_when_written INTEGER,
        not_misleading_clearly_satire INTEGER,
        not_misleading_personal_opinion INTEGER,
        trustworthy_sources INTEGER,
        summary TEXT,
        is_media_note INTEGER,
        is_collaborative_note INTEGER
    )
    """)
    conn.commit()
    conn.close()
    print("DB created")

def parse_tsv(file_path: str) -> List[NoteData]:
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    print(f"File: {file_path} will be parsed")

    note_data_list: List[NoteData] = []
    # ヘッダは回避する
    lines = lines[1:]

    # 行ごとに読み込んでNoteData型に変換する
    for i, line in tenumerate(lines):
        try:
            note_data_list.append(NoteData(*line.strip().split("\t")))
        except ValueError:
            print(f"Note data parsing failed at line {i}")
            print("WARNING: This note will be skipped, but the program will continue")
            continue
    print("Note data parsing completed")
    return note_data_list

def insert_note_data(note_data_list: List[NoteData]) -> None:
    conn = sq3.connect("note_data.sqlite3")
    cursor = conn.cursor()

    # 一応1個ずつやっていく
    for i, note_data in tenumerate(note_data_list):
        try:
            cursor.execute("""
            INSERT INTO note_data (
                note_id,
                note_author_participant_id,
                created_at_millis,
                tweet_id,
                classification,
                believable,
                harmful,
                validation_difficulty,
                misleading_other,
                misleading_factual_error,
                misleading_manipulated_media,
                misleading_outdated_information,
                misleading_missing_important_context,
                misleading_unverified_claim_as_fact,
                misleading_satire,
                not_misleading_other,
                not_misleading_factually_correct,
                not_misleading_outdated_but_not_when_written,
                not_misleading_clearly_satire,
                not_misleading_personal_opinion,
                trustworthy_sources,
                summary,
                is_media_note,
                is_collaborative_note
            ) VALUES (
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?
            )
            """,
            (
                note_data.noteId,
                note_data.noteAuthorParticipantId,
                note_data.createdAtMillis,
                note_data.tweetId,
                note_data.classification,
                note_data.believable,
                note_data.harmful,
                note_data.validationDifficulty,
                note_data.misleadingOther,
                note_data.misleadingFactualError,
                note_data.misleadingManipulatedMedia,
                note_data.misleadingOutdatedInformation,
                note_data.misleadingMissingImportantContext,
                note_data.misleadingUnverifiedClaimAsFact,
                note_data.misleadingSatire,
                note_data.notMisleadingOther,
                note_data.notMisleadingFactuallyCorrect,
                note_data.notMisleadingOutdatedButNotWhenWritten,
                note_data.notMisleadingClearlySatire,
                note_data.notMisleadingPersonalOpinion,
                note_data.trustworthySources,
                note_data.summary,
                note_data.isMediaNote,
                note_data.isCollaborativeNote
            )
        )
        except:
            print(f"Note data insertion failed at line {i}")
            print("WARNING: This note will be skipped, but the program will continue")
            continue

def main() -> None:
    Make_DB()

    # datasにあるすべての notes-*****.tsvファイルのパスを取得
    path_list = []
    for root, dirs, files in os.walk("datas"):
        for file in files:
            if file.startswith("notes-") and file.endswith(".tsv"):
                path_list.append(os.path.join(root, file))

    # 各ファイルを順番に解析してDBに挿入
    for path in path_list:
        note_data_list = parse_tsv(path)
        insert_note_data(note_data_list)

if __name__ == "__main__":
    main()
