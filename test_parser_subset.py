import sqlite3 as sq3
import os
import csv
import sys
from typing import List, Optional, Generator
from tqdm import tqdm
import datetime as dt

# Increase CSV field size limit for large fields
csv.field_size_limit(sys.maxsize)

class NoteData:
    def __init__(self, row: dict):
        self.noteId = int(float(row.get('noteId', -1) or -1))
        self.noteAuthorParticipantId = row.get('noteAuthorParticipantId', "")
        self.createdAtMillis = int(float(row.get('createdAtMillis', 0) or 0))
        self.tweetId = int(float(row.get('tweetId', -1) or -1))
        self.classification = row.get('classification', "")
        self.believable = row.get('believable', "")
        self.harmful = row.get('harmful', "")
        self.validationDifficulty = row.get('validationDifficulty', "")
        self.misleadingOther = int(float(row.get('misleadingOther', 0) or 0))
        self.misleadingFactualError = int(float(row.get('misleadingFactualError', 0) or 0))
        self.misleadingManipulatedMedia = int(float(row.get('misleadingManipulatedMedia', 0) or 0))
        self.misleadingOutdatedInformation = int(float(row.get('misleadingOutdatedInformation', 0) or 0))
        self.misleadingMissingImportantContext = int(float(row.get('misleadingMissingImportantContext', 0) or 0))
        self.misleadingUnverifiedClaimAsFact = int(float(row.get('misleadingUnverifiedClaimAsFact', 0) or 0))
        self.misleadingSatire = int(float(row.get('misleadingSatire', 0) or 0))
        self.notMisleadingOther = int(float(row.get('notMisleadingOther', 0) or 0))
        self.notMisleadingFactuallyCorrect = int(float(row.get('notMisleadingFactuallyCorrect', 0) or 0))
        self.notMisleadingOutdatedButNotWhenWritten = int(float(row.get('notMisleadingOutdatedButNotWhenWritten', 0) or 0))
        self.notMisleadingClearlySatire = int(float(row.get('notMisleadingClearlySatire', 0) or 0))
        self.notMisleadingPersonalOpinion = int(float(row.get('notMisleadingPersonalOpinion', 0) or 0))
        self.trustworthySources = int(float(row.get('trustworthySources', 0) or 0))
        self.summary = row.get('summary', "")
        self.isMediaNote = int(float(row.get('isMediaNote', 0) or 0))
        self.isCollaborativeNote = int(float(row.get('isCollaborativeNote', 0) or 0))

class NoteStatusHistoryData:
    def __init__(self, row: dict):
        self.noteId = int(float(row.get('noteId', -1) or -1))
        self.participantId = row.get('participantId', row.get('noteAuthorParticipantId', ""))
        self.createdAtMillis = int(float(row.get('createdAtMillis', 0) or 0))
        self.timestampMillisOfFirstNonNMRStatus = int(float(row.get('timestampMillisOfFirstNonNMRStatus', 0) or 0))
        self.firstNonNMRStatus = row.get('firstNonNMRStatus', "")
        self.timestampMillisOfCurrentStatus = int(float(row.get('timestampMillisOfCurrentStatus', 0) or 0))
        self.currentStatus = row.get('currentStatus', "")
        self.timestampMillisOfLatestNonNMRStatus = int(float(row.get('timestampMillisOfLatestNonNMRStatus', 0) or 0))
        self.latestNonNMRStatus = row.get('latestNonNMRStatus', row.get('mostRecentNonNMRStatus', ""))
        self.timestampMillisOfStatusLock = int(float(row.get('timestampMillisOfStatusLock', 0) or 0))
        self.lockedStatus = row.get('lockedStatus', "")
        self.timestampMillisOfRetroLock = int(float(row.get('timestampMillisOfRetroLock', 0) or 0))
        self.currentCoreStatus = row.get('currentCoreStatus', "")
        self.currentExpansionStatus = row.get('currentExpansionStatus', "")
        self.currentGroupStatus = row.get('currentGroupStatus', "")
        self.currentDecidedByKey = row.get('currentDecidedByKey', row.get('currentDecidedBy', ""))
        self.currentModelingGroup = int(float(row.get('currentModelingGroup', 0) or 0))
        self.timestampMillisOfMostRecentStatusChange = int(float(row.get('timestampMillisOfMostRecentStatusChange', 0) or 0))
        self.timestampMillisOfNmrDueToMinStableCrhTime = int(float(row.get('timestampMillisOfNmrDueToMinStableCrhTime', 0) or 0))
        self.currentMultiGroupStatus = row.get('currentMultiGroupStatus', "")
        self.currentModelingMultiGroup = int(float(row.get('currentModelingMultiGroup', 0) or 0))
        self.timestampMinuteOfFinalScoringOutput = row.get('timestampMinuteOfFinalScoringOutput', "")
        self.timestampMillisOfFirstNmrDueToMinStableCrhTime = int(float(row.get('timestampMillisOfFirstNmrDueToMinStableCrhTime', 0) or 0))

class RatingsData:
    def __init__(self, row: dict):
        self.noteId = int(float(row.get('noteId', -1) or -1))
        self.participantId = row.get('participantId', row.get('raterParticipantId', ""))
        self.createdAtMillis = int(float(row.get('createdAtMillis', 0) or 0))
        self.ratedOnTweetId = int(float(row.get('ratedOnTweetId', -1) or -1))
        self.ratingSourceBucketed = row.get('ratingSourceBucketed', "")
        self.suggestion = row.get('suggestion', "")
        self.helpfulnessLevel = row.get('helpfulnessLevel', "")
        self.helpfulOther = int(float(row.get('helpfulOther', 0) or 0))
        self.helpfulInformative = int(float(row.get('helpfulInformative', 0) or 0))
        self.helpfulClear = int(float(row.get('helpfulClear', 0) or 0))
        self.helpfulEmpathetic = int(float(row.get('helpfulEmpathetic', 0) or 0))
        self.helpfulGoodSources = int(float(row.get('helpfulGoodSources', 0) or 0))
        self.helpfulUniqueContext = int(float(row.get('helpfulUniqueContext', 0) or 0))
        self.helpfulAddressesClaim = int(float(row.get('helpfulAddressesClaim', 0) or 0))
        self.helpfulImportantContext = int(float(row.get('helpfulImportantContext', 0) or 0))
        self.helpfulUnbiasedLanguage = int(float(row.get('helpfulUnbiasedLanguage', 0) or 0))
        self.notHelpfulOther = int(float(row.get('notHelpfulOther', 0) or 0))
        self.notHelpfulIncorrect = int(float(row.get('notHelpfulIncorrect', 0) or 0))
        self.notHelpfulSourcesMissingOrUnreliable = int(float(row.get('notHelpfulSourcesMissingOrUnreliable', 0) or 0))
        self.notHelpfulOpinionSpeculationOrBias = int(float(row.get('notHelpfulOpinionSpeculationOrBias', 0) or 0))
        self.notHelpfulMissingKeyPoints = int(float(row.get('notHelpfulMissingKeyPoints', 0) or 0))
        self.notHelpfulOutdated = int(float(row.get('notHelpfulOutdated', 0) or 0))
        self.notHelpfulHardToUnderstand = int(float(row.get('notHelpfulHardToUnderstand', 0) or 0))
        self.notHelpfulArgumentativeOrBiased = int(float(row.get('notHelpfulArgumentativeOrBiased', 0) or 0))
        self.notHelpfulOffTopic = int(float(row.get('notHelpfulOffTopic', 0) or 0))
        self.notHelpfulSpamHarassmentOrAbuse = int(float(row.get('notHelpfulSpamHarassmentOrAbuse', 0) or 0))
        self.notHelpfulIrrelevantSources = int(float(row.get('notHelpfulIrrelevantSources', 0) or 0))
        self.notHelpfulOpinionSpeculation = int(float(row.get('notHelpfulOpinionSpeculation', 0) or 0))
        self.notHelpfulNoteNotNeeded = int(float(row.get('notHelpfulNoteNotNeeded', 0) or 0))

class UserEnrollmentData:
    def __init__(self, row: dict):
        self.participantId = row.get('participantId', "")
        self.enrollmentState = row.get('enrollmentState', "")
        self.successfulRatingNeededToEarnIn = int(float(row.get('successfulRatingNeededToEarnIn', 0) or 0))
        self.timestampOfLastStateChange = int(float(row.get('timestampOfLastStateChange', 0) or 0))
        self.timestampOfLastEarnOut = int(float(row.get('timestampOfLastEarnOut', 0) or 0))
        self.modelingPopulation = row.get('modelingPopulation', "")
        self.modelingGroup = int(float(row.get('modelingGroup', 0) or 0))

class BatSignalsData:
    def __init__(self, row: dict):
        self.tweetId = int(float(row.get('tweetId', -1) or -1))
        self.sourceLinks = row.get('sourceLinks', "")
        self.noteRequestFeedEligibleTimestamp = int(float(row.get('noteRequestFeedEligibleAtMillis', row.get('noteRequestFeedEligibleTimestamp', 0)) or -1))
        self.apiSmallFeedEligibleTimestamp = int(float(row.get('apiSmallFeedEligibleAtMillis', row.get('apiSmallFeedEligibleTimestamp', 0)) or -1))
        self.apiLargeFeedEligibleTimestamp = int(float(row.get('apiLargeFeedEligibleAtMillis', row.get('apiLargeFeedEligibleTimestamp', 0)) or -1))

def Make_DB() -> str:
    print("Creating DB...")
    db_path = f"test_subset_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite3"
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""CREATE TABLE note_data (noteId INTEGER PRIMARY KEY, noteAuthorParticipantId TEXT, createdAtMillis INTEGER, tweetId INTEGER, classification TEXT, believable TEXT, harmful TEXT, validationDifficulty TEXT, misleadingOther INTEGER, misleadingFactualError INTEGER, misleadingManipulatedMedia INTEGER, misleadingOutdatedInformation INTEGER, misleadingMissingImportantContext INTEGER, misleadingUnverifiedClaimAsFact INTEGER, misleadingSatire INTEGER, notMisleadingOther INTEGER, notMisleadingFactuallyCorrect INTEGER, notMisleadingOutdatedButNotWhenWritten INTEGER, notMisleadingClearlySatire INTEGER, notMisleadingPersonalOpinion INTEGER, trustworthySources INTEGER, summary TEXT, isMediaNote INTEGER, isCollaborativeNote INTEGER)""")
    cursor.execute("""CREATE TABLE note_status_history (noteId INTEGER, participantId TEXT, createdAtMillis INTEGER, timestampMillisOfFirstNonNMRStatus INTEGER, firstNonNMRStatus TEXT, timestampMillisOfCurrentStatus INTEGER, currentStatus TEXT, timestampMillisOfLatestNonNMRStatus INTEGER, latestNonNMRStatus TEXT, timestampMillisOfStatusLock INTEGER, lockedStatus TEXT, timestampMillisOfRetroLock INTEGER, currentCoreStatus TEXT, currentExpansionStatus TEXT, currentGroupStatus TEXT, currentDecidedByKey TEXT, currentModelingGroup INTEGER, timestampMillisOfMostRecentStatusChange INTEGER, timestampMillisOfNmrDueToMinStableCrhTime INTEGER, currentMultiGroupStatus TEXT, currentModelingMultiGroup INTEGER, timestampMinuteOfFinalScoringOutput TEXT, timestampMillisOfFirstNmrDueToMinStableCrhTime INTEGER, FOREIGN KEY (noteId) REFERENCES note_data(noteId))""")
    cursor.execute("""CREATE TABLE ratings (noteId INTEGER, participantId TEXT, createdAtMillis INTEGER, ratedOnTweetId INTEGER, ratingSourceBucketed TEXT, suggestion TEXT, helpfulnessLevel TEXT, helpfulOther INTEGER, helpfulInformative INTEGER, helpfulClear INTEGER, helpfulEmpathetic INTEGER, helpfulGoodSources INTEGER, helpfulUniqueContext INTEGER, helpfulAddressesClaim INTEGER, helpfulImportantContext INTEGER, helpfulUnbiasedLanguage INTEGER, notHelpfulOther INTEGER, notHelpfulIncorrect INTEGER, notHelpfulSourcesMissingOrUnreliable INTEGER, notHelpfulOpinionSpeculationOrBias INTEGER, notHelpfulMissingKeyPoints INTEGER, notHelpfulOutdated INTEGER, notHelpfulHardToUnderstand INTEGER, notHelpfulArgumentativeOrBiased INTEGER, notHelpfulOffTopic INTEGER, notHelpfulSpamHarassmentOrAbuse INTEGER, notHelpfulIrrelevantSources INTEGER, notHelpfulOpinionSpeculation INTEGER, notHelpfulNoteNotNeeded INTEGER, PRIMARY KEY (noteId, participantId), FOREIGN KEY (noteId) REFERENCES note_data(noteId))""")
    cursor.execute("""CREATE TABLE user_enrollment (participantId TEXT PRIMARY KEY, enrollmentState TEXT, successfulRatingNeededToEarnIn INTEGER, timestampOfLastStateChange INTEGER, timestampOfLastEarnOut INTEGER, modelingPopulation TEXT, modelingGroup INTEGER)""")
    cursor.execute("""CREATE TABLE bat_signals (tweetId INTEGER PRIMARY KEY, sourceLinks TEXT, noteRequestFeedEligibleTimestamp INTEGER, apiSmallFeedEligibleTimestamp INTEGER, apiLargeFeedEligibleTimestamp INTEGER)""")
    
    conn.commit()
    conn.close()
    return db_path

def process_tsv_subset(file_path: str, data_class, insert_func, db_path: str, limit: int = 1000, encoding="utf-8") -> None:
    print(f"File: {file_path} (subset {limit})")
    conn = sq3.connect(db_path)
    cursor = conn.cursor()
    try:
        with open(file_path, "r", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter='\t')
            batch = []
            count = 0
            for row in reader:
                try:
                    batch.append(data_class(row))
                    count += 1
                except Exception:
                    pass
                if count >= limit:
                    break
            if batch:
                insert_func(cursor, batch)
                conn.commit()
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
    finally:
        conn.close()

def insert_note_data(cursor, note_data_list):
    values = [(n.noteId, n.noteAuthorParticipantId, n.createdAtMillis, n.tweetId, n.classification, n.believable, n.harmful, n.validationDifficulty, n.misleadingOther, n.misleadingFactualError, n.misleadingManipulatedMedia, n.misleadingOutdatedInformation, n.misleadingMissingImportantContext, n.misleadingUnverifiedClaimAsFact, n.misleadingSatire, n.notMisleadingOther, n.notMisleadingFactuallyCorrect, n.notMisleadingOutdatedButNotWhenWritten, n.notMisleadingClearlySatire, n.notMisleadingPersonalOpinion, n.trustworthySources, n.summary, n.isMediaNote, n.isCollaborativeNote) for n in note_data_list]
    cursor.executemany("INSERT OR IGNORE INTO note_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)

def insert_note_status_history(cursor, data_list):
    values = [(n.noteId, n.participantId, n.createdAtMillis, n.timestampMillisOfFirstNonNMRStatus, n.firstNonNMRStatus, n.timestampMillisOfCurrentStatus, n.currentStatus, n.timestampMillisOfLatestNonNMRStatus, n.latestNonNMRStatus, n.timestampMillisOfStatusLock, n.lockedStatus, n.timestampMillisOfRetroLock, n.currentCoreStatus, n.currentExpansionStatus, n.currentGroupStatus, n.currentDecidedByKey, n.currentModelingGroup, n.timestampMillisOfMostRecentStatusChange, n.timestampMillisOfNmrDueToMinStableCrhTime, n.currentMultiGroupStatus, n.currentModelingMultiGroup, n.timestampMinuteOfFinalScoringOutput, n.timestampMillisOfFirstNmrDueToMinStableCrhTime) for n in data_list]
    cursor.executemany("INSERT OR IGNORE INTO note_status_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)

def insert_ratings(cursor, data_list):
    values = [(n.noteId, n.participantId, n.createdAtMillis, n.ratedOnTweetId, n.ratingSourceBucketed, n.suggestion, n.helpfulnessLevel, n.helpfulOther, n.helpfulInformative, n.helpfulClear, n.helpfulEmpathetic, n.helpfulGoodSources, n.helpfulUniqueContext, n.helpfulAddressesClaim, n.helpfulImportantContext, n.helpfulUnbiasedLanguage, n.notHelpfulOther, n.notHelpfulIncorrect, n.notHelpfulSourcesMissingOrUnreliable, n.notHelpfulOpinionSpeculationOrBias, n.notHelpfulMissingKeyPoints, n.notHelpfulOutdated, n.notHelpfulHardToUnderstand, n.notHelpfulArgumentativeOrBiased, n.notHelpfulOffTopic, n.notHelpfulSpamHarassmentOrAbuse, n.notHelpfulIrrelevantSources, n.notHelpfulOpinionSpeculation, n.notHelpfulNoteNotNeeded) for n in data_list]
    cursor.executemany("INSERT OR IGNORE INTO ratings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)

def insert_user_enrollment(cursor, data_list):
    values = [(n.participantId, n.enrollmentState, n.successfulRatingNeededToEarnIn, n.timestampOfLastStateChange, n.timestampOfLastEarnOut, n.modelingPopulation, n.modelingGroup) for n in data_list]
    cursor.executemany("INSERT OR IGNORE INTO user_enrollment VALUES (?,?,?,?,?,?,?)", values)

def insert_bat_signals(cursor, data_list):
    values = [(n.tweetId, n.sourceLinks, n.noteRequestFeedEligibleTimestamp, n.apiSmallFeedEligibleTimestamp, n.apiLargeFeedEligibleTimestamp) for n in data_list]
    cursor.executemany("INSERT OR IGNORE INTO bat_signals VALUES (?,?,?,?,?)", values)

def main():
    db_path = Make_DB()
    for root, dirs, files in os.walk("datas"):
        for file in files:
            path = os.path.join(root, file)
            if file.startswith("notes-") and file.endswith(".tsv"):
                process_tsv_subset(path, NoteData, insert_note_data, db_path)
            elif file.startswith("noteStatusHistory-") and file.endswith(".tsv"):
                process_tsv_subset(path, NoteStatusHistoryData, insert_note_status_history, db_path)
            # elif file.startswith("ratings-") and file.endswith(".tsv"):
            #     process_tsv_subset(path, RatingsData, insert_ratings, db_path)
            # elif file.startswith("userEnrollment-") and file.endswith(".tsv"):
            #     process_tsv_subset(path, UserEnrollmentData, insert_user_enrollment, db_path)
            # elif file.startswith("batSignals-") and file.endswith(".tsv"):
            #     process_tsv_subset(path, BatSignalsData, insert_bat_signals, db_path)
    print(f"Test DB created at {db_path}")

if __name__ == "__main__":
    main()
