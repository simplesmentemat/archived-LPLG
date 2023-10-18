from msgspec import Struct
from typing import List,Optional,Dict

class SeasonInfo(Struct):
    seasonId: int
    seasonName: str
    startTime: str
    endTime: str
    openStatus: bool

class StageInfo(Struct):
    stageId: int
    stageName: str
    startTime: str
    endTime: str

class StageData(Struct):
    seasonId: int
    seasonName: str
    stageInfos: list[StageInfo]

class Match(Struct):
    matchId: int
    startTime: str
    teamAId: int
    teamAName: str
    teamBId:int
    teamBName:str
    bo: int
    status:int
    winTeamId:int
    stageId:int
    stageName:str

class MatchResponse(Struct):
    data: List[Match]

class StageResponse(Struct):
    data: StageData

class ScheduleResponse(Struct):
    data: List[SeasonInfo]

#details
class Item(Struct):
    itemId: int
    itemName: str

class PerkRune(Struct):
    runeId: int
    iconKey: str

class battleDetail(Struct):
    kills: int
    death: int
    assist: int
    kda:float
    highestKDA: bool
    highestKillStreak: int
    highestMultiKill: int
    attendWarRate: float
    highestAttendWarRate: bool

class damageDetail(Struct):
    heroDamage: float
    heroPhysicalDamage: float
    heroMagicalDamage: float
    heroTrueDamage:float
    totalDamage:float
    totalPhysicalDamage:float
    totalMagicalDamage:float
    totalTrueDamage:float
    highestCritDamage:float
    damageTransit:float
    highestDamageTransit:bool
    damagePerGold:float
    highestDamagePerGold:bool
    damageRate:float
    highestDamageRate:bool

class DamageTakenDetail(Struct):
    damageTaken:float
    physicalDamageTaken:float
    magicalDamageTaken:float
    trueDamageTaken:float
    takenDamageRate:float
    highestTakenDamageRate:bool
    damageTakenPerGold:float

class otherDetail(Struct):
    golds: int
    oppositeGoldsDiff: int
    turretAmount: int
    creepsKilled: int
    level: int
    firstBlood: bool
    firstTurret: bool
    firstTurretKill: bool
    firstTurretAssist: bool
    spentGold: int
    totalNeutralMinKilled: float
    totalMinKilledYourJungle: float
    totalMinKilledEnemyJungle: float

class visionDetail(Struct):
    wardPlaced: int
    wardKilled: int
    visionScore: float
    highestVisionScore: bool
    controlWardPurchased: int

class perkStyle(Struct):
    styleId: int
    styleEn: str

class perkSubStyle(Struct):
    styleId: int
    styleEn: str


class trinketItem(Struct):
    itemId: int
    itemName: str

class PlayerInfo(Struct):
    playerId: int
    playerName: str
    playerAvatar: str
    heroId: int
    heroName: str
    heroTitle: str
    spell1Name: str
    spell1Id: int
    spell1IconKey: str
    spell2Name: str
    spell2Id: int
    spell2IconKey: str
    accountId: int
    trinketItem: trinketItem
    items: List[Item]
    minionKilled: int
    perkStyle: perkStyle
    perkSubStyle: perkSubStyle
    perkRunes: List[PerkRune]
    battleDetail: battleDetail
    damageDetail: damageDetail
    DamageTakenDetail: DamageTakenDetail
    otherDetail: otherDetail
    visionDetail: visionDetail

class TeamInfo(Struct):
    teamId: int
    kills: int
    baronAmount: int
    dragonAmount: int
    turretAmount: int
    golds: int
    banHeroList: List[int]
    playerInfos: List[PlayerInfo]

class MatchInfo(Struct):
    bo: int
    teamAId: int
    teamBId: int
    matchStartTime: str
    matchEndTime: str
    matchWin: int
    matchStatus: int
    blueTeam: int
    teamInfos: List[TeamInfo]

class MatchData(Struct):
    matchId: int
    matchName: str
    seasonId: int
    seasonName: str
    stageId: int
    stageName: str
    gameMode: str
    matchTime: str
    matchStatus: int
    matchWin: int
    teamAId: int
    teamAName: str
    teamAScore: int
    teamBId: int
    teamBName: str
    teamBScore: int
    matchInfos: List[MatchInfo]

class DetailsResponse(Struct):
    data: MatchData

