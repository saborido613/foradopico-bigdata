"""
ForaDoPico - DAG de coleta (Global Solution 2026.2 - Big Data Architecture & Data Integration)

Coleta diaria de 4 fontes, em paralelo, gravando um arquivo por coleta na pasta chegada/:
  1. demanda_metro : entradas diarias por estacao do Metro SP, em milhares (dados abertos do
                     portal de transparencia do Metro, consolidados em CSV pelo projeto metrosp,
                     licenca MIT; o portal oficial publica em PDF)
  2. clima         : chuva e temperatura por hora em Sao Paulo (Open-Meteo, sem chave)
  3. feriados      : feriados nacionais do ano (BrasilAPI, sem chave)
  4. escala        : escala presencial de uma empresa FICTICIA (dados simulados, com nomes e
                     e-mails inventados, usados para demonstrar a protecao LGPD na camada Ouro)

Requisitos atendidos: 02 (DAG agendado gravando em chegada/ com a data no nome) e
03 (tarefas em paralelo, tarefa de conferencia, notificacao de falha e chaves em Variables).
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import random
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

log = logging.getLogger(__name__)

# Pasta de pouso (mapeada para a pasta chegada/ do seu computador no docker-compose.yaml)
CHEGADA = Path("/opt/airflow/chegada")

FONTES = ["demanda_metro", "clima", "feriados", "escala"]


def _var(nome: str, padrao: str = "") -> str:
    """Le uma Airflow Variable. Nada de chave, URL secreta ou coordenada fixa no codigo."""
    return Variable.get(nome, default=padrao)


def _destino(fonte: str, ds: str, extensao: str) -> Path:
    destino = CHEGADA / fonte / f"{fonte}_{ds}.{extensao}"
    destino.parent.mkdir(parents=True, exist_ok=True)
    return destino


# ---------------------------------------------------------------------------
# 1. Demanda do Metro (fonte principal)
# ---------------------------------------------------------------------------
def coletar_demanda_metro(ds: str, **_) -> None:
    # CSV de entradas diarias por estacao (Linhas 1, 2, 3 e 15), em MILHARES de passageiros.
    # Origem: portal de transparencia do Metro SP, consolidado pelo projeto metrosp (MIT).
    url = _var(
        "FORADOPICO_METRO_CSV_URL",
        "https://raw.githubusercontent.com/viniciusoike/metrosp/main/"
        "data-raw/outputs/processed/metro_sp_station_daily_current.csv",
    )
    arquivo = requests.get(url, timeout=180)
    arquivo.raise_for_status()
    conteudo = arquivo.content
    if url.endswith(".gz"):
        conteudo = gzip.decompress(conteudo)
    leitor = csv.DictReader(io.StringIO(conteudo.decode("utf-8-sig")))

    linhas_mvp = {s.strip() for s in _var("FORADOPICO_LINHAS", "1,3").split(",")}
    data_inicio = _var("FORADOPICO_DEMANDA_INICIO", "2026-01-01")

    saida = []
    for linha in leitor:
        if linha.get("line_number") not in linhas_mvp:
            continue
        if not (data_inicio <= linha.get("date", "") <= ds):
            continue
        saida.append({
            "data": linha["date"],
            "linha": linha["line_number"],
            "estacao_codigo": linha.get("station_code", ""),
            "estacao_nome": linha.get("station_name", ""),
            "entradas_mil": linha.get("passengers", ""),  # valor bruto, em milhares
            "data_coleta": ds,
        })

    if not saida:
        raise ValueError("Nenhuma linha de demanda depois do filtro. Confira as Variables de linhas e data.")

    destino = _destino("demanda_metro", ds, "csv")
    with destino.open("w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=list(saida[0].keys()))
        escritor.writeheader()
        escritor.writerows(saida)
    log.info("demanda_metro: %s linhas gravadas em %s", len(saida), destino)


# ---------------------------------------------------------------------------
# 2. Clima por hora (complemento: chuva aumenta a lotacao)
# ---------------------------------------------------------------------------
def coletar_clima(ds: str, **_) -> None:
    resposta = requests.get(
        _var("FORADOPICO_CLIMA_API", "https://api.open-meteo.com/v1/forecast"),
        params={
            "latitude": _var("FORADOPICO_LATITUDE", "-23.55"),
            "longitude": _var("FORADOPICO_LONGITUDE", "-46.63"),
            "start_date": ds,
            "end_date": ds,
            "hourly": "temperature_2m,precipitation",
            "timezone": "America/Sao_Paulo",
        },
        timeout=30,
    )
    resposta.raise_for_status()
    destino = _destino("clima", ds, "json")
    destino.write_text(resposta.text, encoding="utf-8")
    log.info("clima: gravado em %s", destino)


# ---------------------------------------------------------------------------
# 3. Feriados do ano (marca dias de demanda baixa)
# ---------------------------------------------------------------------------
def coletar_feriados(ds: str, **_) -> None:
    base = _var("FORADOPICO_FERIADOS_API", "https://brasilapi.com.br/api/feriados/v1")
    resposta = requests.get(f"{base}/{ds[:4]}", timeout=30)
    resposta.raise_for_status()
    destino = _destino("feriados", ds, "json")
    destino.write_text(json.dumps(resposta.json(), ensure_ascii=False), encoding="utf-8")
    log.info("feriados: gravado em %s", destino)


# ---------------------------------------------------------------------------
# 4. Escala presencial da empresa ficticia (dados simulados)
# ---------------------------------------------------------------------------
PRIMEIROS_NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Hugo",
                   "Isabela", "Joao", "Karina", "Lucas", "Marina", "Nicolas", "Olivia", "Paulo",
                   "Rafaela", "Samuel", "Tatiane", "Vitor", "Yasmin", "Rodrigo", "Leticia", "Caio"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Lima", "Pereira", "Costa", "Almeida",
              "Ferreira", "Rodrigues", "Gomes", "Martins"]
# Estacoes de embarque das Linhas 1-Azul e 3-Vermelha (escopo do MVP)
ESTACOES = [("1", "Tucuruvi"), ("1", "Santana"), ("1", "Jabaquara"), ("1", "Saúde"),
            ("1", "Ana Rosa"), ("1", "Luz"), ("3", "Corinthians-Itaquera"), ("3", "Artur Alvim"),
            ("3", "Penha"), ("3", "Tatuapé"), ("3", "Brás"), ("3", "Palmeiras-Barra Funda")]
TIMES = {"Financeiro": 12, "Tecnologia": 16}


def _funcionarios() -> list[dict]:
    """Quadro fixo de funcionarios ficticios (mesma semente = mesmas pessoas em toda coleta)."""
    rnd = random.Random(42)
    pessoas, n = [], 1
    for time, tamanho in TIMES.items():
        for _ in range(tamanho):
            nome = f"{rnd.choice(PRIMEIROS_NOMES)} {rnd.choice(SOBRENOMES)}"
            linha, estacao = rnd.choice(ESTACOES)
            pessoas.append({
                "funcionario_id": f"F{n:03d}",
                "nome": nome,
                "email": f"{nome.lower().replace(' ', '.')}.{n}@empresa-ficticia.com.br",
                "time": time,
                "linha_embarque": linha,
                "estacao_embarque": estacao,
            })
            n += 1
    return pessoas


def gerar_escala(ds: str, **_) -> None:
    dia = pendulum.parse(ds)
    destino = _destino("escala", ds, "csv")
    campos = ["data", "empresa_id", "time", "funcionario_id", "nome", "email",
              "linha_embarque", "estacao_embarque", "presencial", "janela_entrada", "data_coleta"]

    linhas = []
    if dia.day_of_week < 5:  # segunda (0) a sexta (4); fim de semana gera arquivo so com cabecalho
        rnd = random.Random(ds)
        fechamento = dia.day >= 25 or dia.day <= 3  # semana de fechamento pede mais gente
        for pessoa in _funcionarios():
            chance = 0.75 if fechamento else 0.40
            linha = {
                "data": ds,
                "empresa_id": "EMP001",
                **pessoa,
                "presencial": str(rnd.random() < chance).lower(),
                "janela_entrada": "08:00",  # escala original do RH: todo mundo no pico
                "data_coleta": ds,
            }
            # Ruido proposital (~4%) simulando erro de digitacao do RH, para as regras de qualidade da Prata
            sorteio = rnd.random()
            if sorteio < 0.02:
                linha["estacao_embarque"] = ""
            elif sorteio < 0.04:
                linha["janela_entrada"] = "25:00"
            linhas.append(linha)

    with destino.open("w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(linhas)
    log.info("escala: %s linhas gravadas em %s", len(linhas), destino)


# ---------------------------------------------------------------------------
# Conferencia e notificacao
# ---------------------------------------------------------------------------
def conferir_chegada(ds: str, **_) -> None:
    faltando = []
    for fonte in FONTES:
        arquivos = list((CHEGADA / fonte).glob(f"{fonte}_{ds}.*"))
        if not arquivos or arquivos[0].stat().st_size == 0:
            faltando.append(fonte)
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios em {ds}: {faltando}")
    log.info("Todos os arquivos de %s chegaram: %s", ds, FONTES)


def notificar_falha(context) -> None:
    """Envia aviso de falha para um webhook (Discord, por exemplo). A URL fica em Variable."""
    ti = context.get("task_instance")
    mensagem = (f"ForaDoPico: falha na tarefa {getattr(ti, 'task_id', '?')} "
                f"do DAG {getattr(ti, 'dag_id', '?')} em {context.get('ds')}.")
    url = _var("FORADOPICO_WEBHOOK_URL")
    if not url:
        log.warning("%s (sem webhook configurado, so registrando no log)", mensagem)
        return
    try:
        requests.post(url, json={"content": mensagem}, timeout=10)
    except requests.RequestException as erro:
        log.warning("Nao foi possivel notificar: %s", erro)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="foradopico_coleta",
    description="Coleta diaria das fontes do ForaDoPico para a pasta chegada/",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notificar_falha,
    },
    tags=["foradopico", "gs-2026", "big-data"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    coletas = [
        PythonOperator(task_id="coletar_demanda_metro", python_callable=coletar_demanda_metro),
        PythonOperator(task_id="coletar_clima", python_callable=coletar_clima),
        PythonOperator(task_id="coletar_feriados", python_callable=coletar_feriados),
        PythonOperator(task_id="gerar_escala", python_callable=gerar_escala),
    ]

    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    inicio >> coletas >> conferir
