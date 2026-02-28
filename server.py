from fastapi import FastAPI, APIRouter, HTTPException
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
from bson import ObjectId
from emergentintegrations.llm.chat import LlmChat, UserMessage
import secrets
import string

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

EMERGENT_LLM_KEY = os.environ.get('EMERGENT_LLM_KEY', '')

app = FastAPI(title="Pumas FC Control API")
api_router = APIRouter(prefix="/api")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== CONSTANTS ==========

POSITIONS = ["POR", "DFC", "LI", "LD", "MCD", "MC", "MCO", "CAR", "EI", "ED", "DC", "MP"]
POSITION_NAMES = {
    "POR": "Portero", "DFC": "Defensa Central", "LI": "Lateral Izquierdo", "LD": "Lateral Derecho",
    "MCD": "Mediocentro Defensivo", "MC": "Mediocentro", "MCO": "Mediocentro Ofensivo",
    "CAR": "Carrilero", "EI": "Extremo Izquierdo", "ED": "Extremo Derecho", "DC": "Delantero Centro", "MP": "Media Punta"
}

FORMATIONS = {
    "3-1-4-2": {"name": "3-1-4-2", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "DFC", "x": 20, "y": 78}, {"pos": "DFC", "x": 50, "y": 78},
        {"pos": "DFC", "x": 80, "y": 78}, {"pos": "MCD", "x": 50, "y": 60}, {"pos": "CAR", "x": 8, "y": 45},
        {"pos": "MC", "x": 35, "y": 45}, {"pos": "MC", "x": 65, "y": 45}, {"pos": "CAR", "x": 92, "y": 45},
        {"pos": "DC", "x": 35, "y": 18}, {"pos": "DC", "x": 65, "y": 18}
    ]},
    "3-5-2": {"name": "3-5-2", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "DFC", "x": 20, "y": 78}, {"pos": "DFC", "x": 50, "y": 78},
        {"pos": "DFC", "x": 80, "y": 78}, {"pos": "CAR", "x": 8, "y": 55}, {"pos": "MC", "x": 30, "y": 55},
        {"pos": "MC", "x": 50, "y": 55}, {"pos": "MC", "x": 70, "y": 55}, {"pos": "CAR", "x": 92, "y": 55},
        {"pos": "DC", "x": 35, "y": 18}, {"pos": "DC", "x": 65, "y": 18}
    ]},
    "4-4-2": {"name": "4-4-2", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "LI", "x": 10, "y": 75}, {"pos": "DFC", "x": 35, "y": 78},
        {"pos": "DFC", "x": 65, "y": 78}, {"pos": "LD", "x": 90, "y": 75}, {"pos": "EI", "x": 10, "y": 50},
        {"pos": "MC", "x": 35, "y": 55}, {"pos": "MC", "x": 65, "y": 55}, {"pos": "ED", "x": 90, "y": 50},
        {"pos": "DC", "x": 35, "y": 18}, {"pos": "DC", "x": 65, "y": 18}
    ]},
    "4-3-3": {"name": "4-3-3", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "LI", "x": 10, "y": 75}, {"pos": "DFC", "x": 35, "y": 78},
        {"pos": "DFC", "x": 65, "y": 78}, {"pos": "LD", "x": 90, "y": 75}, {"pos": "MC", "x": 30, "y": 55},
        {"pos": "MC", "x": 50, "y": 55}, {"pos": "MC", "x": 70, "y": 55}, {"pos": "EI", "x": 15, "y": 22},
        {"pos": "DC", "x": 50, "y": 15}, {"pos": "ED", "x": 85, "y": 22}
    ]},
    "4-2-3-1": {"name": "4-2-3-1", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "LI", "x": 10, "y": 75}, {"pos": "DFC", "x": 35, "y": 78},
        {"pos": "DFC", "x": 65, "y": 78}, {"pos": "LD", "x": 90, "y": 75}, {"pos": "MCD", "x": 35, "y": 60},
        {"pos": "MCD", "x": 65, "y": 60}, {"pos": "EI", "x": 15, "y": 40}, {"pos": "MCO", "x": 50, "y": 38},
        {"pos": "ED", "x": 85, "y": 40}, {"pos": "DC", "x": 50, "y": 15}
    ]},
    "5-3-2": {"name": "5-3-2", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "CAR", "x": 5, "y": 70}, {"pos": "DFC", "x": 25, "y": 78},
        {"pos": "DFC", "x": 50, "y": 78}, {"pos": "DFC", "x": 75, "y": 78}, {"pos": "CAR", "x": 95, "y": 70},
        {"pos": "MC", "x": 30, "y": 50}, {"pos": "MC", "x": 50, "y": 50}, {"pos": "MC", "x": 70, "y": 50},
        {"pos": "DC", "x": 35, "y": 18}, {"pos": "DC", "x": 65, "y": 18}
    ]},
    "4-1-4-1": {"name": "4-1-4-1", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "LI", "x": 10, "y": 75}, {"pos": "DFC", "x": 35, "y": 78},
        {"pos": "DFC", "x": 65, "y": 78}, {"pos": "LD", "x": 90, "y": 75}, {"pos": "MCD", "x": 50, "y": 62},
        {"pos": "EI", "x": 10, "y": 45}, {"pos": "MC", "x": 35, "y": 45}, {"pos": "MC", "x": 65, "y": 45},
        {"pos": "ED", "x": 90, "y": 45}, {"pos": "DC", "x": 50, "y": 15}
    ]},
    "4-5-1": {"name": "4-5-1", "positions": [
        {"pos": "POR", "x": 50, "y": 92}, {"pos": "LI", "x": 10, "y": 75}, {"pos": "DFC", "x": 35, "y": 78},
        {"pos": "DFC", "x": 65, "y": 78}, {"pos": "LD", "x": 90, "y": 75}, {"pos": "EI", "x": 10, "y": 50},
        {"pos": "MC", "x": 30, "y": 55}, {"pos": "MC", "x": 50, "y": 55}, {"pos": "MC", "x": 70, "y": 55},
        {"pos": "ED", "x": 90, "y": 50}, {"pos": "DC", "x": 50, "y": 15}
    ]},
}

PLAYER_ROLES = ["Titular Frecuente", "Titular Rotación", "Suplente Clave", "Reserva"]
FORM_STATES = ["Excelente", "Bueno", "Normal", "Bajo", "Muy bajo"]
TRENDS = ["Mejorando", "Estable", "Decayendo"]
FAULT_TYPES = ["Leve", "Media", "Grave"]
EVENT_TYPES = ["Partido", "Entrenamiento", "Reunión", "Otro"]

# ========== MODELS ==========

class Team(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "Pumas FC"
    share_code: str = Field(default_factory=lambda: ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6)))
    created_at: datetime = Field(default_factory=datetime.utcnow)

class PlayerStats(BaseModel):
    matches_played: int = 0
    goals: int = 0
    assists: int = 0
    clean_sheets: int = 0
    key_errors: int = 0
    total_rating_sum: float = 0.0
    ratings_count: int = 0
    last_5_ratings: List[float] = []
    wins_contributed: int = 0

class PlayerBase(BaseModel):
    real_name: str
    nickname: str
    primary_position: str
    secondary_positions: List[str] = []
    current_rating: float = 70.0
    current_role: str = "Reserva"
    current_form: str = "Normal"
    discipline_level: int = 100
    private_notes: str = ""

class PlayerCreate(PlayerBase):
    pass

class PlayerUpdate(BaseModel):
    real_name: Optional[str] = None
    nickname: Optional[str] = None
    primary_position: Optional[str] = None
    secondary_positions: Optional[List[str]] = None
    current_rating: Optional[float] = None
    current_role: Optional[str] = None
    current_form: Optional[str] = None
    discipline_level: Optional[int] = None
    private_notes: Optional[str] = None

class Player(PlayerBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    stats: PlayerStats = Field(default_factory=PlayerStats)
    trend: str = "Estable"
    versatility: int = 1
    impact_on_wins: float = 0.0
    avg_rating_last_5: float = 0.0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# Discipline Models - Con motivo personalizado
class DisciplineRecordBase(BaseModel):
    player_id: str
    fault_type: str  # Leve, Media, Grave
    reason: str  # Motivo personalizado
    decision: str  # Qué se decidió
    is_active: bool = True  # Si está activa o ya fue resuelta

class DisciplineRecordCreate(DisciplineRecordBase):
    pass

class DisciplineRecord(DisciplineRecordBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    date: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None

# Event/Calendar Models
class EventBase(BaseModel):
    title: str
    event_type: str = "Partido"  # Partido, Entrenamiento, Reunión, Otro
    date: datetime
    opponent: Optional[str] = None  # Para partidos
    location: Optional[str] = None
    notes: Optional[str] = None
    is_important: bool = False

class EventCreate(EventBase):
    pass

class Event(EventBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Match Models
class MatchPlayerPerformance(BaseModel):
    player_id: str
    position_played: str
    rating: float
    goals: int = 0
    assists: int = 0
    clean_sheet: bool = False
    key_errors: int = 0

class MatchBase(BaseModel):
    opponent: str
    result: str
    score_for: int
    score_against: int
    formation_used: str = "3-1-4-2"
    notes: str = ""
    performances: List[MatchPlayerPerformance] = []

class Match(MatchBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    date: datetime = Field(default_factory=datetime.utcnow)

# Lineup Models
class LineupPosition(BaseModel):
    slot_index: int
    player_id: Optional[str] = None
    position: str
    x: float
    y: float

class LineupBase(BaseModel):
    name: str
    formation_type: str = "3-1-4-2"
    positions: List[LineupPosition] = []
    bench: List[str] = []
    captain_id: Optional[str] = None
    penalty_taker_id: Optional[str] = None
    freekick_taker_id: Optional[str] = None
    corner_taker_id: Optional[str] = None
    is_active: bool = False

class Lineup(LineupBase):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

# AI Models
class AIQueryRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    session_type: str = "live"

class AIMessageBase(BaseModel):
    role: str
    content: str

class AIConversation(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    session_type: str
    messages: List[AIMessageBase] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Dashboard
class DashboardStats(BaseModel):
    avg_team_form: float
    discipline_index: float
    best_performer: Optional[Dict[str, Any]] = None
    players_improving: List[Dict[str, Any]] = []
    players_declining: List[Dict[str, Any]] = []
    recent_results: List[Dict[str, Any]] = []
    lineup_stability: float
    total_players: int
    upcoming_events: List[Dict[str, Any]] = []
    active_faults: int = 0

# ========== HELPERS ==========

def calculate_player_metrics(player: dict) -> dict:
    stats = player.get('stats', {})
    positions = [player.get('primary_position', '')] + player.get('secondary_positions', [])
    versatility = len([p for p in positions if p])
    last_5 = stats.get('last_5_ratings', [])
    avg_last_5 = sum(last_5) / len(last_5) if last_5 else 0.0
    wins = stats.get('wins_contributed', 0)
    matches = stats.get('matches_played', 0)
    impact = (wins / matches * 100) if matches > 0 else 0.0
    trend = "Estable"
    if len(last_5) >= 3:
        recent = last_5[-3:]
        if all(recent[i] < recent[i+1] for i in range(len(recent)-1)):
            trend = "Mejorando"
        elif all(recent[i] > recent[i+1] for i in range(len(recent)-1)):
            trend = "Decayendo"
    return {'versatility': versatility, 'avg_rating_last_5': round(avg_last_5, 2), 'impact_on_wins': round(impact, 2), 'trend': trend}

async def get_or_create_team():
    team = await db.team.find_one({})
    if not team:
        new_team = Team()
        team_dict = new_team.model_dump()
        await db.team.insert_one(team_dict)
        return team_dict
    if '_id' in team:
        del team['_id']
    return team

async def get_team_context():
    players = await db.players.find().to_list(100)
    matches = await db.matches.find().sort("date", -1).limit(10).to_list(10)
    discipline = await db.discipline_records.find({"is_active": True}).to_list(50)
    
    # Create player lookup map for O(1) access (fixes N+1 query issue)
    player_map = {p['id']: p for p in players}
    
    context = {"total_players": len(players), "players": [], "recent_matches": [], "discipline_alerts": []}
    
    for p in players:
        metrics = calculate_player_metrics(p)
        context["players"].append({
            "name": p.get('nickname', p.get('real_name', '')),
            "position": p.get('primary_position', ''),
            "rating": p.get('current_rating', 0),
            "form": p.get('current_form', 'Normal'),
            "role": p.get('current_role', 'Reserva'),
            "trend": metrics['trend']
        })
    
    for m in matches:
        context["recent_matches"].append({
            "opponent": m.get('opponent', ''),
            "result": m.get('result', ''),
            "score": f"{m.get('score_for', 0)}-{m.get('score_against', 0)}"
        })
    
    for d in discipline:
        player = player_map.get(d.get('player_id'))
        if player:
            context["discipline_alerts"].append({
                "player": player.get('nickname', ''),
                "type": d.get('fault_type', ''),
                "reason": d.get('reason', '')
            })
    
    return context

# ========== ROUTES ==========

@api_router.get("/")
async def root():
    return {"message": "Pumas FC Control API", "version": "3.0.0"}

@api_router.get("/health")
async def health():
    return {"status": "healthy"}

# Team
@api_router.get("/team")
async def get_team():
    team = await get_or_create_team()
    return team

@api_router.post("/team/join/{share_code}")
async def join_team(share_code: str):
    team = await db.team.find_one({"share_code": share_code.upper()})
    if not team:
        raise HTTPException(status_code=404, detail="Código no válido")
    return {"success": True, "message": "¡Te has unido al equipo!"}

@api_router.post("/team/regenerate-code")
async def regenerate_share_code():
    new_code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    await db.team.update_one({}, {"$set": {"share_code": new_code}})
    return {"share_code": new_code}

# Players
@api_router.post("/players", response_model=Player)
async def create_player(player_data: PlayerCreate):
    player = Player(**player_data.model_dump())
    player.versatility = len([player.primary_position] + player.secondary_positions)
    await db.players.insert_one(player.model_dump())
    return player

@api_router.get("/players")
async def get_players(sort_by: str = "current_rating", order: str = "desc"):
    sort_order = -1 if order == "desc" else 1
    sort_field = {"performance": "avg_rating_last_5", "impact": "impact_on_wins", "discipline": "discipline_level"}.get(sort_by, sort_by)
    
    players = await db.players.find().sort(sort_field, sort_order).to_list(100)
    result = []
    for p in players:
        if '_id' in p:
            del p['_id']
        metrics = calculate_player_metrics(p)
        p.update(metrics)
        result.append(p)
    return result

@api_router.get("/players/{player_id}")
async def get_player(player_id: str):
    player = await db.players.find_one({"id": player_id})
    if not player:
        raise HTTPException(status_code=404, detail="Jugador no encontrado")
    if '_id' in player:
        del player['_id']
    metrics = calculate_player_metrics(player)
    player.update(metrics)
    
    # Get active faults for this player
    active_faults = await db.discipline_records.find({"player_id": player_id, "is_active": True}).to_list(50)
    for f in active_faults:
        if '_id' in f:
            del f['_id']
    player['active_faults'] = active_faults
    
    return player

@api_router.put("/players/{player_id}")
async def update_player(player_id: str, player_update: PlayerUpdate):
    update_data = {k: v for k, v in player_update.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No hay datos")
    update_data['updated_at'] = datetime.utcnow()
    result = await db.players.update_one({"id": player_id}, {"$set": update_data})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Jugador no encontrado")
    player = await db.players.find_one({"id": player_id})
    if '_id' in player:
        del player['_id']
    return player

@api_router.delete("/players/{player_id}")
async def delete_player(player_id: str):
    result = await db.players.delete_one({"id": player_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Jugador no encontrado")
    await db.discipline_records.delete_many({"player_id": player_id})
    return {"message": "Jugador eliminado"}

# Discipline - Con motivos personalizados
@api_router.post("/discipline")
async def create_discipline_record(record: DisciplineRecordCreate):
    player = await db.players.find_one({"id": record.player_id})
    if not player:
        raise HTTPException(status_code=404, detail="Jugador no encontrado")
    
    discipline_record = DisciplineRecord(**record.model_dump())
    await db.discipline_records.insert_one(discipline_record.model_dump())
    
    # Update player discipline level
    fault_penalties = {"Leve": 5, "Media": 15, "Grave": 30}
    penalty = fault_penalties.get(record.fault_type, 5)
    new_level = max(0, player.get('discipline_level', 100) - penalty)
    await db.players.update_one({"id": record.player_id}, {"$set": {"discipline_level": new_level}})
    
    # Check accumulation
    active_records = await db.discipline_records.find({"player_id": record.player_id, "is_active": True}).to_list(100)
    leve_count = sum(1 for r in active_records if r.get('fault_type') == 'Leve')
    media_count = sum(1 for r in active_records if r.get('fault_type') == 'Media')
    
    if leve_count > 0 and leve_count % 3 == 0:
        auto_media = DisciplineRecord(
            player_id=record.player_id,
            fault_type="Media",
            reason="Acumulación de 3 faltas leves",
            decision="Sanción automática",
            is_active=True
        )
        await db.discipline_records.insert_one(auto_media.model_dump())
    
    if media_count > 0 and media_count % 2 == 0:
        auto_grave = DisciplineRecord(
            player_id=record.player_id,
            fault_type="Grave",
            reason="Acumulación de 2 faltas medias",
            decision="Sanción automática",
            is_active=True
        )
        await db.discipline_records.insert_one(auto_grave.model_dump())
    
    return {"id": discipline_record.id, "message": "Falta registrada"}

@api_router.get("/discipline")
async def get_discipline_records(player_id: Optional[str] = None, active_only: bool = False):
    query = {}
    if player_id:
        query["player_id"] = player_id
    if active_only:
        query["is_active"] = True
    records = await db.discipline_records.find(query).sort("date", -1).to_list(200)
    for r in records:
        if '_id' in r:
            del r['_id']
    return records

@api_router.get("/discipline/index")
async def get_discipline_index():
    players = await db.players.find().to_list(100)
    if not players:
        return {"index": 100, "at_risk": [], "active_faults": 0}
    total = sum(p.get('discipline_level', 100) for p in players)
    index = total / len(players)
    at_risk = [{"id": p['id'], "name": p.get('nickname', ''), "level": p.get('discipline_level', 100)} for p in players if p.get('discipline_level', 100) < 50]
    active_faults = await db.discipline_records.count_documents({"is_active": True})
    return {"index": round(index, 2), "at_risk": at_risk, "active_faults": active_faults}

@api_router.put("/discipline/{fault_id}/resolve")
async def resolve_fault(fault_id: str):
    """Marca una falta como resuelta/quitada"""
    result = await db.discipline_records.update_one(
        {"id": fault_id},
        {"$set": {"is_active": False, "resolved_at": datetime.utcnow()}}
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Falta no encontrada")
    
    # Recalculate player discipline
    fault = await db.discipline_records.find_one({"id": fault_id})
    if fault:
        player = await db.players.find_one({"id": fault.get('player_id')})
        if player:
            # Restore some discipline when fault is resolved
            fault_penalties = {"Leve": 5, "Media": 15, "Grave": 30}
            restore = fault_penalties.get(fault.get('fault_type'), 5)
            new_level = min(100, player.get('discipline_level', 100) + restore)
            await db.players.update_one({"id": player['id']}, {"$set": {"discipline_level": new_level}})
    
    return {"message": "Falta resuelta"}

@api_router.delete("/discipline/{fault_id}")
async def delete_fault(fault_id: str):
    """Elimina una falta completamente"""
    fault = await db.discipline_records.find_one({"id": fault_id})
    if not fault:
        raise HTTPException(status_code=404, detail="Falta no encontrada")
    
    # Restore discipline
    player = await db.players.find_one({"id": fault.get('player_id')})
    if player and fault.get('is_active'):
        fault_penalties = {"Leve": 5, "Media": 15, "Grave": 30}
        restore = fault_penalties.get(fault.get('fault_type'), 5)
        new_level = min(100, player.get('discipline_level', 100) + restore)
        await db.players.update_one({"id": player['id']}, {"$set": {"discipline_level": new_level}})
    
    await db.discipline_records.delete_one({"id": fault_id})
    return {"message": "Falta eliminada"}

# Events/Calendar
@api_router.post("/events")
async def create_event(event_data: EventCreate):
    event = Event(**event_data.model_dump())
    await db.events.insert_one(event.model_dump())
    return {"id": event.id, "message": "Evento creado"}

@api_router.get("/events")
async def get_events(upcoming_only: bool = False):
    query = {}
    if upcoming_only:
        query["date"] = {"$gte": datetime.utcnow()}
    events = await db.events.find(query).sort("date", 1).to_list(100)
    for e in events:
        if '_id' in e:
            del e['_id']
    return events

@api_router.get("/events/{event_id}")
async def get_event(event_id: str):
    event = await db.events.find_one({"id": event_id})
    if not event:
        raise HTTPException(status_code=404, detail="Evento no encontrado")
    if '_id' in event:
        del event['_id']
    return event

@api_router.put("/events/{event_id}")
async def update_event(event_id: str, event_data: EventCreate):
    update_dict = event_data.model_dump()
    result = await db.events.update_one({"id": event_id}, {"$set": update_dict})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Evento no encontrado")
    return {"message": "Evento actualizado"}

@api_router.delete("/events/{event_id}")
async def delete_event(event_id: str):
    result = await db.events.delete_one({"id": event_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Evento no encontrado")
    return {"message": "Evento eliminado"}

# Matches
@api_router.post("/matches")
async def create_match(match_data: MatchBase):
    match = Match(**match_data.model_dump())
    await db.matches.insert_one(match.model_dump())
    
    is_win = match.result == "Victoria"
    for perf in match.performances:
        player = await db.players.find_one({"id": perf.player_id})
        if player:
            stats = player.get('stats', {})
            stats['matches_played'] = stats.get('matches_played', 0) + 1
            stats['goals'] = stats.get('goals', 0) + perf.goals
            stats['assists'] = stats.get('assists', 0) + perf.assists
            stats['key_errors'] = stats.get('key_errors', 0) + perf.key_errors
            if perf.clean_sheet:
                stats['clean_sheets'] = stats.get('clean_sheets', 0) + 1
            if is_win:
                stats['wins_contributed'] = stats.get('wins_contributed', 0) + 1
            last_5 = stats.get('last_5_ratings', [])
            last_5.append(perf.rating)
            stats['last_5_ratings'] = last_5[-5:]
            
            if len(stats['last_5_ratings']) >= 3:
                avg = sum(stats['last_5_ratings'][-3:]) / 3
                new_form = "Excelente" if avg >= 8 else "Bueno" if avg >= 7 else "Normal" if avg >= 5.5 else "Bajo" if avg >= 4 else "Muy bajo"
            else:
                new_form = player.get('current_form', 'Normal')
            
            await db.players.update_one({"id": perf.player_id}, {"$set": {"stats": stats, "current_form": new_form}})
    
    return {"id": match.id, "message": "Partido registrado"}

@api_router.get("/matches")
async def get_matches(limit: int = 20):
    matches = await db.matches.find().sort("date", -1).limit(limit).to_list(limit)
    for m in matches:
        if '_id' in m:
            del m['_id']
    return matches

# Formations & Lineups
@api_router.get("/formations")
async def get_formations():
    return FORMATIONS

@api_router.get("/lineups/auto/best-xi")
async def get_best_xi(criteria: str = "performance", formation: str = "3-1-4-2"):
    players = await db.players.find().to_list(100)
    formation_data = FORMATIONS.get(formation, FORMATIONS["3-1-4-2"])
    
    for p in players:
        if '_id' in p:
            del p['_id']
        metrics = calculate_player_metrics(p)
        p.update(metrics)
        if criteria == "performance":
            p['score'] = p.get('avg_rating_last_5', 0) * 0.6 + p.get('current_rating', 0) * 0.4
        elif criteria == "impact":
            p['score'] = p.get('impact_on_wins', 0)
        elif criteria == "form":
            form_scores = {"Excelente": 10, "Bueno": 8, "Normal": 6, "Bajo": 4, "Muy bajo": 2}
            p['score'] = form_scores.get(p.get('current_form', 'Normal'), 6)
        else:
            p['score'] = p.get('current_rating', 0)
    
    selected = []
    remaining = list(players)
    
    for i, pos_data in enumerate(formation_data["positions"]):
        pos = pos_data["pos"]
        candidates = [p for p in remaining if p.get('primary_position') == pos or pos in p.get('secondary_positions', [])]
        if not candidates:
            candidates = remaining.copy() if remaining else []
        candidates.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        if candidates:
            player = candidates[0]
            selected.append({
                "slot_index": i, "player_id": player['id'], "name": player.get('nickname', ''),
                "position": pos, "rating": player.get('current_rating', 0),
                "form": player.get('current_form', 'Normal'), "score": player.get('score', 0),
                "x": pos_data["x"], "y": pos_data["y"]
            })
            remaining.remove(player)
        else:
            selected.append({
                "slot_index": i, "player_id": None, "name": None,
                "position": pos, "rating": None, "form": None, "score": None,
                "x": pos_data["x"], "y": pos_data["y"]
            })
    
    remaining.sort(key=lambda x: x.get('score', 0), reverse=True)
    bench = [{"player_id": p['id'], "name": p.get('nickname', ''), "position": p.get('primary_position', ''), "rating": p.get('current_rating', 0)} for p in remaining[:7]]
    
    return {"formation": formation, "criteria": criteria, "players": selected, "bench": bench}

# Dashboard
@api_router.get("/dashboard")
async def get_dashboard():
    players = await db.players.find().to_list(100)
    matches = await db.matches.find().sort("date", -1).limit(5).to_list(5)
    events = await db.events.find({"date": {"$gte": datetime.utcnow()}}).sort("date", 1).limit(3).to_list(3)
    active_faults = await db.discipline_records.count_documents({"is_active": True})
    
    if not players:
        return DashboardStats(avg_team_form=0, discipline_index=100, best_performer=None, players_improving=[], players_declining=[], recent_results=[], lineup_stability=0, total_players=0, upcoming_events=[], active_faults=0)
    
    form_scores = {"Excelente": 10, "Bueno": 8, "Normal": 6, "Bajo": 4, "Muy bajo": 2}
    avg_form = sum(form_scores.get(p.get('current_form', 'Normal'), 6) for p in players) / len(players)
    discipline_index = sum(p.get('discipline_level', 100) for p in players) / len(players)
    
    for p in players:
        metrics = calculate_player_metrics(p)
        p.update(metrics)
    
    best = max(players, key=lambda x: x.get('avg_rating_last_5', 0))
    best_performer = {"id": best['id'], "name": best.get('nickname', ''), "position": best.get('primary_position', ''), "avg_rating": best.get('avg_rating_last_5', 0), "form": best.get('current_form', 'Normal')}
    
    improving = [{"id": p['id'], "name": p.get('nickname', '')} for p in players if p.get('trend') == 'Mejorando'][:5]
    declining = [{"id": p['id'], "name": p.get('nickname', '')} for p in players if p.get('trend') == 'Decayendo'][:5]
    
    recent_results = [{"opponent": m.get('opponent', ''), "result": m.get('result', ''), "score": f"{m.get('score_for', 0)}-{m.get('score_against', 0)}", "date": m.get('date', datetime.utcnow()).isoformat()} for m in matches]
    
    upcoming = [{"id": e.get('id'), "title": e.get('title', ''), "event_type": e.get('event_type', ''), "date": e.get('date', datetime.utcnow()).isoformat(), "opponent": e.get('opponent', '')} for e in events]
    
    starters = len([p for p in players if p.get('current_role') in ['Titular Frecuente', 'Titular Rotación']])
    stability = min((starters / 11 * 100), 100)
    
    return DashboardStats(avg_team_form=round(avg_form, 2), discipline_index=round(discipline_index, 2), best_performer=best_performer, players_improving=improving, players_declining=declining, recent_results=recent_results, lineup_stability=round(stability, 2), total_players=len(players), upcoming_events=upcoming, active_faults=active_faults)

# AI
@api_router.post("/ai/analyze")
async def ai_analyze(request: AIQueryRequest):
    if not EMERGENT_LLM_KEY:
        raise HTTPException(status_code=500, detail="IA no configurada")
    
    context = await get_team_context()
    system_message = f"""Eres el asistente táctico de Pumas FC (Pro Clubs EA FC 25). Analiza y da consejos concretos en español.

PLANTILLA ({context['total_players']} jugadores):
""" + '\n'.join([f"- {p['name']} ({p['position']}): Rating {p['rating']}, {p['form']}, {p['role']}" for p in context['players']]) + f"""

ÚLTIMOS RESULTADOS: {', '.join([f"{m['result']} vs {m['opponent']} ({m['score']})" for m in context['recent_matches'][:5]]) or 'Sin partidos'}

ALERTAS: {', '.join([f"{a['player']}: {a['type']} - {a['reason']}" for a in context['discipline_alerts'][:3]]) or 'Ninguna'}

Responde siempre en español, sé conciso y práctico."""

    try:
        chat = LlmChat(api_key=EMERGENT_LLM_KEY, session_id=request.session_id or str(uuid.uuid4()), system_message=system_message).with_model("openai", "gpt-4o")
        response = await chat.send_message(UserMessage(text=request.message))
        return {"session_id": request.session_id or str(uuid.uuid4()), "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Constants
@api_router.get("/constants")
async def get_constants():
    return {"positions": POSITIONS, "position_names": POSITION_NAMES, "player_roles": PLAYER_ROLES, "form_states": FORM_STATES, "trends": TRENDS, "fault_types": FAULT_TYPES, "event_types": EVENT_TYPES, "formations": list(FORMATIONS.keys())}

@api_router.get("/test-mongo")
async def test_mongo():
    try:
        count = await db.players.count_documents({})
        return {"success": True, "players_count": count}
    except Exception as e:
        return {"success": False, "error": str(e)}

app.include_router(api_router)
app.add_middleware(CORSMiddleware, allow_credentials=True, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
