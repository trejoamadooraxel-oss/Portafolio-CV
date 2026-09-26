
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from actions_for_inf import union_informacion


app = FastAPI()

class InfClientSchema(BaseModel):
    nombre: str
    apellido_materno: str
    apellido_paterno: str
    edad: int


@app.get('/inf/get/cliente')
async def regreso_datos(datos:InfClientSchema = Depends()):

    cadena = union_informacion(
        datos.nombre,
        datos.apellido_materno,
        datos.apellido_paterno,
        datos.edad
    )

    return cadena
