from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def read_covid_data():
    pass