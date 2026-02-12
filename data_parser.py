import sqlite3 as sq3
import os
import csv
import sys
from typing import List, Optional
from tqdm import tqdm
import datetime as dt

# Increase CSV field size limit for large fields
csv.field_size_limit(sys.maxsize)

class NoteData:
    def __init__(self, row: dict):
        self.noteId = int(row.get('noteId', -1))
        self.noteAuthorParticipantId = row.get('noteAuthorParticipantId', "")
        self.createdAtMillis = int(row.get('createdAtMillis', 0))
        self.tweetId = int(row.get('tweetId', -1))
        self.classification = row.get('classification', "")
        self.believable = row.get('believable', "")
        self.harmful = row.get('harmful', "")
        self.validationDifficulty = row.get('validationDifficulty', "")
        self.misleadingOther = int(row.get('misleadingOther', 0) or 0)
        self.misleadingFactualError = int(row.get('misleadingFactualError', 0) or 0)
        self.misleadingManipulatedMedia = int(row.get('misleadingManipulatedMedia', 0) or 0)
        self.misleadingOutdatedInformation = int(row.get('misleadingOutdatedInformation', 0) or 0)
        self.misleadingMissingImportantContext = int(row.get('misleadingMissingImportantContext', 0) or 0)
        self.misleadingUnverifiedClaimAsFact = int(row.get('misleadingUnverifiedClaimAsFact', 0) or 0)
        self.misleadingSatire = int(row.get('misleadingSatire', 0) or 0)
        self.notMisleadingOther = int(row.get('notMisleadingOther', 0) or 0)
        self.notMisleadingFactuallyCorrect = int(row.get('notMisleadingFactuallyCorrect', 0) or 0)
        self.notMisleadingOutdatedButNotWhenWritten = int(row.get('notMisleadingOutdatedButNotWhenWritten', 0) or 0)
        self.notMisleadingClearlySatire = int(row.get('notMisleadingClearlySatire', 0) or 0)
        self.notMisleadingPersonalOpinion = int(row.get('notMisleadingPersonalOpinion', 0) or 0)
        self.trustworthySources = int(row.get('trustworthySources', 0) or 0)
        self.summary = row.get('summary', "")
        self.isMediaNote = int(row.get('isMediaNote', 0) or 0)
        self.isCollaborativeNote = int(row.get('isCollaborativeNote', 0) or 0)

class NoteStatusHistoryData:
    def __init__(self, row: dict):
        self.noteId = int(row.get('noteId', -1))
        self.participantId = row.get('participantId', "")
        self.createdAtMillis = int(row.get('createdAtMillis', 0))
        self.timestampMillisOfFirstNonNMRStatus = int(row.get('timestampMillisOfFirstNonNMRStatus', 0) or 0)
        self.firstNonNMRStatus = row.get('firstNonNMRStatus', "")
        self.timestampMillisOfCurrentStatus = int(row.get('timestampMillisOfCurrentStatus', 0) or 0)
        self.currentStatus = row.get('currentStatus', "")
        self.timestampMillisOfLatestNonNMRStatus = int(row.get('timestampMillisOfLatestNonNMRStatus', 0) or 0)
        self.latestNonNMRStatus = row.get('latestNonNMRStatus', "")
        self.timestampMillisOfStatusLock = int(row.get('timestampMillisOfStatusLock', 0) or 0)
        self.lockedStatus = row.get('lockedStatus', "")
        self.timestampMillisOfRetroLock = int(row.get('timestampMillisOfRetroLock', 0) or 0)
        self.currentCoreStatus = row.get('currentCoreStatus', "")
        self.currentExpansionStatus = row.get('currentExpansionStatus', "")
        self.currentGroupStatus = row.get('currentGroupStatus', "")
        self.currentDecidedByKey = row.get('currentDecidedByKey', "")
        self.currentModelingGroup = int(row.get('currentModelingGroup', 0) or 0)
        self.timestampMillisOfMostRecentStatusChange = int(row.get('timestampMillisOfMostRecentStatusChange', 0) or 0)
        self.timestampMillisOfNmrDueToMinStableCrhTime = int(row.get('timestampMillisOfNmrDueToMinStableCrhTime', 0) or 0)
        self.currentMultiGroupStatus = row.get('currentMultiGroupStatus', "")
        self.currentModelingMultiGroup = int(row.get('currentModelingMultiGroup', 0) or 0)
        self.timestampMinuteOfFinalScoringOutput = row.get('timestampMinuteOfFinalScoringOutput', "")
        self.timestampMillisOfFirstNmrDueToMinStableCrhTime = int(row.get('timestampMillisOfFirstNmrDueToMinStableCrhTime', 0) or 0)

class RatingsData:
    def __init__(self, row: dict):
        self.noteId = int(row.get('noteId', -1))
        # Handle renaming raterParticipantId -> participantId
        self.participantId = row.get('participantId', row.get('raterParticipantId', ""))
        self.createdAtMillis = int(row.get('createdAtMillis', 0))
        self.ratedOnTweetId = int(row.get('ratedOnTweetId', -1))
        self.ratingSourceBucketed = row.get('ratingSourceBucketed', "")
        self.suggestion = row.get('suggestion', "")
        
        self.helpfulnessLevel = row.get('helpfulnessLevel', "")
        self.helpfulOther = int(row.get('helpfulOther', 0) or 0)
        self.helpfulInformative = int(row.get('helpfulInformative', 0) or 0)
        self.helpfulClear = int(row.get('helpfulClear', 0) or 0)
        self.helpfulEmpathetic = int(row.get('helpfulEmpathetic', 0) or 0)
        self.helpfulGoodSources = int(row.get('helpfulGoodSources', 0) or 0)
        self.helpfulUniqueContext = int(row.get('helpfulUniqueContext', 0) or 0)
        self.helpfulAddressesClaim = int(row.get('helpfulAddressesClaim', 0) or 0)
        self.helpfulImportantContext = int(row.get('helpfulImportantContext', 0) or 0)
        self.helpfulUnbiasedLanguage = int(row.get('helpfulUnbiasedLanguage', 0) or 0)
        self.notHelpfulOther = int(row.get('notHelpfulOther', 0) or 0)
        self.notHelpfulIncorrect = int(row.get('notHelpfulIncorrect', 0) or 0)
        self.notHelpfulSourcesMissingOrUnreliable = int(row.get('notHelpfulSourcesMissingOrUnreliable', 0) or 0)
        self.notHelpfulOpinionSpeculationOrBias = int(row.get('notHelpfulOpinionSpeculationOrBias', 0) or 0)
        self.notHelpfulMissingKeyPoints = int(row.get('notHelpfulMissingKeyPoints', 0) or 0)
        self.notHelpfulOutdated = int(row.get('notHelpfulOutdated', 0) or 0)
        self.notHelpfulHardToUnderstand = int(row.get('notHelpfulHardToUnderstand', 0) or 0)
        self.notHelpfulArgumentativeOrBiased = int(row.get('notHelpfulArgumentativeOrBiased', 0) or 0)
        self.notHelpfulOffTopic = int(row.get('notHelpfulOffTopic', 0) or 0)
        self.notHelpfulSpamHarassmentOrAbuse = int(row.get('notHelpfulSpamHarassmentOrAbuse', 0) or 0)
        self.notHelpfulIrrelevantSources = int(row.get('notHelpfulIrrelevantSources', 0) or 0)
        self.notHelpfulOpinionSpeculation = int(row.get('notHelpfulOpinionSpeculation', 0) or 0)
        self.notHelpfulNoteNotNeeded = int(row.get('notHelpfulNoteNotNeeded', 0) or 0)

class UserEnrollmentData:
    def __init__(self, row: dict):
        self.participantId = row.get('participantId', "")
        self.enrollmentState = row.get('enrollmentState', "")
        self.successfulRatingNeededToEarnIn = int(row.get('successfulRatingNeededToEarnIn', 0) or 0)
        self.timestampOfLastStateChange = int(row.get('timestampOfLastStateChange', 0) or 0)
        self.timestampOfLastEarnOut = int(row.get('timestampOfLastEarnOut', 0) or 0)
        self.modelingPopulation = row.get('modelingPopulation', "")
        self.modelingGroup = int(row.get('modelingGroup', 0) or 0)

class BatSignalsData:
    def __init__(self, row: dict):
        self.tweetId = int(row.get('tweetId', -1))
        self.sourceLinks = row.get('sourceLinks', "")
        self.noteRequestFeedEligibleTimestamp = int(row.get('noteRequestFeedEligibleAtMillis', row.get('noteRequestFeedEligibleTimestamp', 0)) or -1)
        self.apiSmallFeedEligibleTimestamp = int(row.get('apiSmallFeedEligibleAtMillis', row.get('apiSmallFeedEligibleTimestamp', 0)) or -1)
        self.apiLargeFeedEligibleTimestamp = int(row.get('apiLargeFeedEligibleAtMillis', row.get('apiLargeFeedEligibleTimestamp', 0)) or -1)

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
        isCollaborativeNote INTEGER
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

def parse_tsv_generic(file_path: str, data_class, encoding="utf-8") -> List:
    print(f"File: {file_path} will be parsed")
    data_list = []
    try:
        with open(file_path, "r", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in tqdm(reader, desc=f"Parsing {os.path.basename(file_path)}"):
                try:
                    data_list.append(data_class(row))
                except Exception as e:
                    pass
    except Exception as e:
        print(f"Failed to parse {file_path}: {e}")
    
    print(f"Parsing completed: {len(data_list)} records")
    return data_list

def insert_note_data(note_data_list: List[NoteData], db_path: str) -> None:
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    values = [(
        n.noteId, n.noteAuthorParticipantId, n.createdAtMillis, n.tweetId, n.classification,
        n.believable, n.harmful, n.validationDifficulty, n.misleadingOther, n.misleadingFactualError,
        n.misleadingManipulatedMedia, n.misleadingOutdatedInformation, n.misleadingMissingImportantContext,
        n.misleadingUnverifiedClaimAsFact, n.misleadingSatire, n.notMisleadingOther, n.notMisleadingFactuallyCorrect,
        n.notMisleadingOutdatedButNotWhenWritten, n.notMisleadingClearlySatire, n.notMisleadingPersonalOpinion,
        n.trustworthySources, n.summary, n.isMediaNote, n.isCollaborativeNote
    ) for n in note_data_list]

    cursor.executemany("""
    INSERT OR IGNORE INTO note_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, values)
    
    conn.commit()
    conn.close()

def insert_note_status_history(data_list: List[NoteStatusHistoryData], db_path: str) -> None:
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    values = [(
        n.noteId, n.participantId, n.createdAtMillis, n.timestampMillisOfFirstNonNMRStatus, n.firstNonNMRStatus,
        n.timestampMillisOfCurrentStatus, n.currentStatus, n.timestampMillisOfLatestNonNMRStatus, n.latestNonNMRStatus,
        n.timestampMillisOfStatusLock, n.lockedStatus, n.timestampMillisOfRetroLock, n.currentCoreStatus,
        n.currentExpansionStatus, n.currentGroupStatus, n.currentDecidedByKey, n.currentModelingGroup,
        n.timestampMillisOfMostRecentStatusChange, n.timestampMillisOfNmrDueToMinStableCrhTime,
        n.currentMultiGroupStatus, n.currentModelingMultiGroup, n.timestampMinuteOfFinalScoringOutput,
        n.timestampMillisOfFirstNmrDueToMinStableCrhTime
    ) for n in data_list]

    cursor.executemany("""
    INSERT OR IGNORE INTO note_status_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, values)
    
    conn.commit()
    conn.close()

def insert_ratings(data_list: List[RatingsData], db_path: str) -> None:
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    values = [(
        n.noteId, n.participantId, n.createdAtMillis, n.ratedOnTweetId, n.ratingSourceBucketed, n.suggestion,
        n.helpfulnessLevel, n.helpfulOther, n.helpfulInformative, n.helpfulClear, n.helpfulEmpathetic,
        n.helpfulGoodSources, n.helpfulUniqueContext, n.helpfulAddressesClaim, n.helpfulImportantContext,
        n.helpfulUnbiasedLanguage, n.notHelpfulOther, n.notHelpfulIncorrect, n.notHelpfulSourcesMissingOrUnreliable,
        n.notHelpfulOpinionSpeculationOrBias, n.notHelpfulMissingKeyPoints, n.notHelpfulOutdated,
        n.notHelpfulHardToUnderstand, n.notHelpfulArgumentativeOrBiased, n.notHelpfulOffTopic,
        n.notHelpfulSpamHarassmentOrAbuse, n.notHelpfulIrrelevantSources, n.notHelpfulOpinionSpeculation,
        n.notHelpfulNoteNotNeeded
    ) for n in data_list]

    cursor.executemany("""
    INSERT OR IGNORE INTO ratings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, values)
    
    conn.commit()
    conn.close()

def insert_user_enrollment(data_list: List[UserEnrollmentData], db_path: str) -> None:
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    values = [(
        n.participantId, n.enrollmentState, n.successfulRatingNeededToEarnIn, n.timestampOfLastStateChange,
        n.timestampOfLastEarnOut, n.modelingPopulation, n.modelingGroup
    ) for n in data_list]

    cursor.executemany("""
    INSERT OR IGNORE INTO user_enrollment VALUES (?,?,?,?,?,?,?)
    """, values)
    
    conn.commit()
    conn.close()

def insert_bat_signals(data_list: List[BatSignalsData], db_path: str) -> None:
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    values = [(
        n.tweetId, n.sourceLinks, n.noteRequestFeedEligibleTimestamp, n.apiSmallFeedEligibleTimestamp,
        n.apiLargeFeedEligibleTimestamp
    ) for n in data_list]

    cursor.executemany("""
    INSERT OR IGNORE INTO bat_signals VALUES (?,?,?,?,?)
    """, values)
    
    conn.commit()
    conn.close()

def make_db_report(db_path: str, path_list: List[str]) -> None:
    with open(f"report_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "w", encoding="utf-8") as f:
        f.write(f"DB Path: {db_path}\n")
        f.write(f"Path List: {path_list}\n")

def main() -> None:
    db_path = Make_DB()

    notes_path_list = []
    ratings_path_list = []
    note_status_history_path_list = []
    user_enrollment_path_list = []
    bat_signals_path_list = []
    for root, dirs, files in os.walk("datas"):
        for file in files:
            if file.startswith("notes-") and file.endswith(".tsv"):
                notes_path_list.append(os.path.join(root, file))
            if file.startswith("noteStatusHistory-") and file.endswith(".tsv"):
                note_status_history_path_list.append(os.path.join(root, file))
            if file.startswith("ratings-") and file.endswith(".tsv"):
                ratings_path_list.append(os.path.join(root, file))
            if file.startswith("userEnrollment-") and file.endswith(".tsv"):
                user_enrollment_path_list.append(os.path.join(root, file))
            if file.startswith("batSignals-") and file.endswith(".tsv"):
                bat_signals_path_list.append(os.path.join(root, file))

    print("Processing notes data...")
    for path in notes_path_list:
        data = parse_tsv_generic(path, NoteData)
        insert_note_data(data, db_path)

    print("Processing note status history data...")
    for path in note_status_history_path_list:
        data = parse_tsv_generic(path, NoteStatusHistoryData)
        insert_note_status_history(data, db_path)

    print("Processing ratings data...")
    for path in ratings_path_list:
        data = parse_tsv_generic(path, RatingsData)
        insert_ratings(data, db_path)

    print("Processing user enrollment data...")
    for path in user_enrollment_path_list:
        data = parse_tsv_generic(path, UserEnrollmentData)
        insert_user_enrollment(data, db_path)

    print("Processing bat signals data...")
    for path in bat_signals_path_list:
        data = parse_tsv_generic(path, BatSignalsData)
        insert_bat_signals(data, db_path)

    make_db_report(db_path, notes_path_list + note_status_history_path_list + ratings_path_list + user_enrollment_path_list + bat_signals_path_list)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
