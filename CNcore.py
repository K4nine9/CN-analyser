
class NoteData:
    def __init__(self, noteId: int, noteAuthorParticipantId: int, createdAtMillis: int, tweetId: int, classification: int, believable: int, harmful: int, validationDifficulty: int, misleadingOther: int, misleadingFactualError: int, misleadingManipulatedMedia: int, misleadingOutdatedInformation: int, misleadingMissingImportantContext: int, misleadingUnverifiedClaimAsFact: int, misleadingSatire: int, notMisleadingOther: int, notMisleadingFactuallyCorrect: int, notMisleadingOutdatedButNotWhenWritten: int, notMisleadingClearlySatire: int, notMisleadingPersonalOpinion: int, trustworthySources: int, summary: str, isMediaNote: int, isCollaborativeNote: int):
        self.noteId = noteId
        self.noteAuthorParticipantId = noteAuthorParticipantId
        self.createdAtMillis = createdAtMillis
        self.tweetId = tweetId
        self.classification = classification
        self.believable = believable
        self.harmful = harmful
        self.validationDifficulty = validationDifficulty
        self.misleadingOther = misleadingOther
        self.misleadingFactualError = misleadingFactualError
        self.misleadingManipulatedMedia = misleadingManipulatedMedia
        self.misleadingOutdatedInformation = misleadingOutdatedInformation
        self.misleadingMissingImportantContext = misleadingMissingImportantContext
        self.misleadingUnverifiedClaimAsFact = misleadingUnverifiedClaimAsFact
        self.misleadingSatire = misleadingSatire
        self.notMisleadingOther = notMisleadingOther
        self.notMisleadingFactuallyCorrect = notMisleadingFactuallyCorrect
        self.notMisleadingOutdatedButNotWhenWritten = notMisleadingOutdatedButNotWhenWritten
        self.notMisleadingClearlySatire = notMisleadingClearlySatire
        self.notMisleadingPersonalOpinion = notMisleadingPersonalOpinion
        self.trustworthySources = trustworthySources
        self.summary = summary
        self.isMediaNote = isMediaNote
        self.isCollaborativeNote = isCollaborativeNote

