from fastapi import FastAPI
from pydantic import BaseModel
import httpx

app = FastAPI()


class InfClientSchema(BaseModel):
    nombre: str
    apellido_materno: str
    apellido_paterno: str
    edad: int


@app.post('/inf/post/cliente')
async def obtener_nombre(datos: InfClientSchema):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            'http://localhost:8002/inf/get/cliente',
                params=datos.dict())
        print("STATUS", response.status_code)
        print("BODY", repr(response.text))
    return response.json()

