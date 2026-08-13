import asyncio
from sqlalchemy import insert, delete, select, text
from conect_sqlalchemy import Base, engine, AsyncSessionLocal

class DB_admin:

    @staticmethod
    async def crear_usuario(nom_user, contrasenia):
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(text(f"CREATE USER {nom_user} WITH PASSWORD '{contrasenia}'"))
                await session.commit()
            except Exception as e:
                await session.rollback()
                print(f"ERROR al otorgar permisos: {e}")


    @staticmethod
    async def otorgar_permisos(nom_user, tabla, permisos):
        permisos_str = ', '.join(permisos)
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(text(f"GRANT {permisos_str} ON TABLE {tabla} TO {nom_user}"))
                await session.commit()
            except Exception as e:
                await session.rollback()
                print(f"ERROR al otorgar permisos: {e}")


    @staticmethod
    async def quitar_permisos(nom_user, tabla, permisos):
        permisos_str = ', '.join(permisos)
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(text(f"REVOKE {permisos_str} ON TABLE {tabla} FROM {nom_user}"))
                await session.commit()
            except Exception as e:
                await session.rollback()
                print(f"ERROR al otorgar permisos: {e}")


    @staticmethod
    async def otorgar_persimos_db(nom_user):
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {nom_user}"))
                await session.commit()
            except Exception as e:
                await session.rollback()
                print(f"ERROR al otorgar permisos: {e}")


    @staticmethod
    async def usar_usuario(nom_user, contrasenia):
        return 0



async def main():
    permisos = ['INSERT', 'UPDATE', 'DELETE']
    await DB_admin.otorgar_persimos_db('omar')

if __name__ == '__main__':
    asyncio.run(main())