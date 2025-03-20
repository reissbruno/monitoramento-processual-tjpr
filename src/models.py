from pydantic import BaseModel
from typing import Optional, List

class Movimentacao(BaseModel):
    seq: Optional[str] = ""
    data: Optional[str] = ""
    evento: Optional[str] = ""
    movimentado_por: Optional[str] = ""


class ResponseSite(BaseModel):
    movimentacoes: List[Movimentacao] = []
    
class ResponseDefault(BaseModel):
    code: int
    message: str
    datetime: str
    results: List[ResponseSite]

class Telemetria(BaseModel):
    tentativas: Optional[int] = 0
    captchas_resolvidos: Optional[int] = 0
    bytes_enviados: Optional[int] = 0
    tempo_total: Optional[float] = 0.0

class ResponseError(BaseModel):
    code: int
    message: str