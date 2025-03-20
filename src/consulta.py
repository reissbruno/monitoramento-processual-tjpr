import time
import re
import logging
from datetime import datetime
from os import environ as env

import httpx
from fastapi import status
from fastapi.responses import JSONResponse
from bs4 import BeautifulSoup
from gradio_client import Client, handle_file
import ddddocr  

# Imports locais
from src.models import Movimentacao, Telemetria

# Configuração do Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constantes
TEMPO_LIMITE = int(env.get('TEMPO_LIMITE', 180))
TENTATIVAS_MAXIMAS_CAPTCHA = int(env.get('TENTATIVAS_MAXIMAS_CAPTCHA', 30))
TENTATIVAS_MAXIMAS_RECURSIVAS = int(env.get('TENTATIVAS_MAXIMAS_RECURSIVAS', 30))
BASE_URL = "https://consulta.tjpr.jus.br"


async def extrair_movimentacoes(soup: BeautifulSoup) -> list[Movimentacao]:
    """
    Extrai as movimentações das tabelas presentes no HTML combinado.
    """
    movimentacoes = []
    tabelas = soup.find_all("table", {"class": "resultTable"})
    for tabela in tabelas:
        linhas = tabela.find_all("tr")[1:]
        for linha in linhas:
            colunas = linha.find_all("td")
            if len(colunas) >= 5:
                seq = colunas[1].get_text(strip=True)
                data = colunas[2].get_text(strip=True)
                evento = " ".join(colunas[3].stripped_strings)

                nome = colunas[4].contents[2].strip() if len(colunas[4].contents) > 2 else ""
                cargo_tag = colunas[4].find("b")
                cargo = cargo_tag.get_text(strip=True) if cargo_tag else ""

                if not nome:
                    nome_tag = colunas[4].find(text=True, recursive=False)
                    nome = nome_tag.strip() if nome_tag else ""

                movimentado_por = f"{nome} - {cargo}" if nome and cargo else (nome or cargo or colunas[4].get_text(strip=True))
                movimentacoes.append(Movimentacao(seq=seq, data=data, evento=evento, movimentado_por=movimentado_por))
    return movimentacoes


def formatar_numero_processo(numero_processo: str) -> str:
    """
    Remove quaisquer caracteres não numéricos do número do processo.
    """
    return ''.join(filter(str.isdigit, numero_processo))


async def obter_soup(client: httpx.AsyncClient, url: str) -> BeautifulSoup:
    """Realiza GET na URL e retorna o BeautifulSoup do conteúdo."""
    resposta = await client.get(url)
    if resposta.status_code != 200:
        raise Exception(f"Falha ao acessar {url}: {resposta.status_code}")
    return BeautifulSoup(resposta.text, "html.parser")


async def resolver_captcha(client: httpx.AsyncClient, url_captcha: str, telemetria: Telemetria) -> str:
    """Faz download e resolve o CAPTCHA utilizando API e fallback OCR."""
    resposta_captcha = await client.get(url_captcha)
    if resposta_captcha.status_code != 200:
        raise Exception(f"Falha ao baixar o CAPTCHA: {resposta_captcha.status_code}")
    bytes_imagem = resposta_captcha.content
    with open("captcha_temporario.png", "wb") as arquivo:
        arquivo.write(bytes_imagem)

    cliente_api = Client("Nischay103/captcha_recognition")
    telemetria.captchas_resolvidos += 1
    try:
        resultado_ocr = cliente_api.predict(
            input=handle_file("captcha_temporario.png"),
            api_name="/predict"
        ).strip()
        logger.info(f"CAPTCHA reconhecido pela API: {resultado_ocr}")
    except Exception as e:
        logger.error(f"Erro ao usar a API captcha_recognition: {e}")
        try:
            motor_ocr = ddddocr.DdddOcr()
            resultado_ocr = motor_ocr.classification(bytes_imagem)
            logger.info(f"CAPTCHA reconhecido pelo ddddocr (fallback): {resultado_ocr}")
        except Exception as fallback_e:
            logger.error(f"Erro no fallback ddddocr: {fallback_e}")
            raise
    return resultado_ocr


def extrair_url_token(script_tags: list, pattern_str: str) -> str:
    """Extrai uma URL/token utilizando regex a partir dos scripts."""
    pattern = re.compile(pattern_str)
    for script in script_tags:
        if script.string:
            match = pattern.search(script.string)
            if match:
                token = match.group(1)
                if "_tj=" in token:
                    return token
    return None


async def fetch(numero_processo: str, telemetria: Telemetria) -> dict:
    """
    Função principal que realiza a consulta. Mantém a estrutura original de tentativas
    e de recursão para resolver o CAPTCHA, e retorna um dicionário com os resultados e telemetria.
    """
    inicio_tempo = time.time()
    if not numero_processo or not isinstance(numero_processo, str):
        telemetria.tempo_total = round(time.time() - inicio_tempo, 2)
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={'code': 2, 'message': 'ERRO_ENTIDADE_NAO_PROCESSAVEL'}
        )

    if telemetria.tentativas >= TENTATIVAS_MAXIMAS_RECURSIVAS:
        logger.error("Número máximo de tentativas recursivas atingido.")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={'code': 3, 'message': 'ERRO_SERVIDOR_INTERNO'}
        )

    logger.info(f'Função fetch() iniciou. Processo: {numero_processo} - Tentativa {telemetria.tentativas}')
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': f'{BASE_URL}/projudi_consulta/'
    }

    results = {}
    try:
        async with httpx.AsyncClient(timeout=TEMPO_LIMITE, headers=headers, follow_redirects=True) as client:
            # 1º GET: Página inicial
            url_pagina_consulta = f"{BASE_URL}/projudi_consulta/"
            resposta_inicial = await client.get(url_pagina_consulta)
            telemetria.bytes_enviados += len(resposta_inicial.text.encode('utf-8'))
            if resposta_inicial.status_code != 200:
                raise Exception(f"Falha ao acessar a página inicial: {resposta_inicial.status_code}")

            soup_inicial = BeautifulSoup(resposta_inicial.text, "html.parser")
            frame = soup_inicial.find("frame", {"id": "mainFrame"})
            if not (frame and frame.get("src")):
                raise Exception("Frame com id='mainFrame' não encontrado ou sem atributo 'src'")
            src = re.sub(r';jsessionid=[^?]*', '', frame["src"])
            url_final = f"{BASE_URL}{src}"
            logger.info(f"URL do mainFrame: {url_final}")

            # 2º GET: cabecalho.jsp
            url_cabecalho = f"{BASE_URL}/projudi_consulta/cabecalho.jsp"
            resposta_cabecalho = await client.get(url_cabecalho)
            telemetria.bytes_enviados += len(resposta_cabecalho.text.encode('utf-8'))
            if resposta_cabecalho.status_code != 200:
                raise Exception(f"Falha ao acessar cabecalho.jsp: {resposta_cabecalho.status_code}")

            client.headers.update({'Referer': url_final})

            # 3º GET: Página de consulta pública (mainFrame)
            resposta_consulta = await client.get(url_final)
            telemetria.bytes_enviados += len(resposta_consulta.text.encode('utf-8'))
            if resposta_consulta.status_code != 200:
                raise Exception(f"Falha ao acessar a página de consulta: {resposta_consulta.status_code}")

            soup_consulta = BeautifulSoup(resposta_consulta.text, "html.parser")
            # Captura da imagem do CAPTCHA
            captcha_img_elem = soup_consulta.find("img", {"id": "captchaImage"})
            if not (captcha_img_elem and captcha_img_elem.get("src")):
                raise Exception("Imagem do CAPTCHA não encontrada ou sem atributo 'src'!")
            url_captcha = f"{BASE_URL}{captcha_img_elem['src']}"
            logger.info(f"URL do CAPTCHA: {url_captcha}")

            # Captura URL AJAX para Autocomplete e demais chamadas
            script_tags = soup_consulta.find_all("script", {"type": "text/javascript"})
            url_ajax = extrair_url_token(script_tags, r'AjaxJspTag\.Select\(\s*"([^"]+)"')
            if not url_ajax:
                raise Exception("URL com '_tj=' não encontrada no HTML")
            if "codComarca" not in url_ajax:
                url_ajax += ("&" if "?" in url_ajax else "?") + "codComarca=-1"
            url_final_ajax = url_ajax if url_ajax.startswith("http") else f"{BASE_URL}{url_ajax}"
            resp_ajax = await client.get(url_final_ajax)
            telemetria.bytes_enviados += len(resp_ajax.text.encode('utf-8'))
            if resp_ajax.status_code != 200:
                raise Exception(f"Falha ao acessar a URL AJAX: {resp_ajax.status_code}")

            url_autocomplete = extrair_url_token(script_tags, r'AjaxJspTag\.Autocomplete\(\s*"([^"]+)"')
            if not url_autocomplete:
                raise Exception("URL do Autocomplete com '_tj=' não encontrada no HTML")
            segunda_url_ajax = url_autocomplete if url_autocomplete.startswith("http") else f"{BASE_URL}{url_autocomplete}"

            payload_segundo_ajax = {
                "numeroProcesso": numero_processo,
                "flagNumeroUnico": "true",
                "opcaoConsulta": "1",
                "_": ""
            }
            ajax_headers = {
                "x-prototype-version": "1.5.1.1",
                "x-requested-with": "XMLHttpRequest"
            }
            resposta_ajax = await client.post(segunda_url_ajax, data=payload_segundo_ajax, headers=ajax_headers)
            telemetria.bytes_enviados += len(resposta_ajax.text.encode('utf-8'))
            if resposta_ajax.status_code != 200:
                raise Exception(f"Falha ao acessar a URL AJAX: {resposta_ajax.status_code}")

            # Resolver CAPTCHA
            resultado_ocr = await resolver_captcha(client, url_captcha, telemetria)

            # Extrair URL de submissão do formulário a partir dos scripts
            script_tags_full = soup_consulta.find_all("script")
            token_url = None
            for script in script_tags_full:
                if script.string:
                    m = re.search(r'document\.getElementById\(["\']buscaProcessoForm["\']\)\.action\s*=\s*["\']([^"\']+)["\']', script.string)
                    if m:
                        relative_url = m.group(1)
                        token_url = relative_url if relative_url.startswith("http") else f"{BASE_URL}{relative_url}"
                        break
            if not token_url:
                raise Exception("URL de submissão do formulário não encontrada.")

            # Submissão final do formulário
            final_headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": BASE_URL,
                "Referer": url_final,
                "x-prototype-version": "1.5.1.1",
                "x-requested-with": "XMLHttpRequest"
            }
            payload_form = {
                "processoPageSize": "20",
                "processoPageNumber": "1",
                "processoSortColumn": "p.numeroUnico",
                "processoSortOrder": "asc",
                "codVaraEscolhida": "",
                "opcaoConsultaPublica": "1",
                "flagNumeroUnico": "true",
                "numeroProcesso": numero_processo,
                "codComarca": "-1",
                "tipoCompetencia": "",
                "turma": "",
                "nomeParte": "",
                "cpfCnpj": "",
                "loginAdvogado": "",
                "nomeAdvogado": "",
                "oab": "",
                "oabComplemento": "N",
                "oabUF": "PR",
                "answer": resultado_ocr
            }
            resposta_formulario = await client.post(token_url, data=payload_form, headers=final_headers)
            telemetria.bytes_enviados += len(resposta_formulario.text.encode('utf-8'))
            if resposta_formulario.status_code != 200:
                raise Exception(f"Falha ao submeter o formulário: {resposta_formulario.status_code}")

            # Chamada AJAX final para obter os resultados
            soup_final = BeautifulSoup(resposta_formulario.text, "html.parser")
            script_tags_final = soup_final.find_all("script", {"type": "text/javascript"})
            token_ajax = extrair_url_token(script_tags_final, r'AjaxJspTag\.HtmlContent\(\s*"([^"]+)"')
            if not token_ajax:
                raise Exception("Token/URL da chamada AjaxJspTag.HtmlContent não encontrado.")
            final_url_ajax = token_ajax if token_ajax.startswith("http") else f"{BASE_URL}{token_ajax}"
            payload_ultimo = {"dummy": "true", "_": ""}
            ultimo_post = await client.post(final_url_ajax, data=payload_ultimo, headers=final_headers)
            telemetria.bytes_enviados += len(ultimo_post.text.encode('utf-8'))
            if ultimo_post.status_code != 200:
                raise Exception(f"Falha ao acessar a URL final: {ultimo_post.status_code}")

            soup_1 = BeautifulSoup(ultimo_post.text, "html.parser")
            # Verifica se há link para mais movimentações
            if "Clique para visualizar as movimentações mais antigas" in soup_1.text:
                script_tags_extra = soup_1.find_all("script", {"type": "text/javascript"})
                url_desejada = extrair_url_token(script_tags_extra, r'AjaxJspTag\.HtmlContent\(\s*"([^"]+)"')
                if url_desejada:
                    full_url = f"{BASE_URL}{url_desejada}"
                    resp_final = await client.post(full_url, data=payload_ultimo, headers=final_headers)
                    telemetria.bytes_enviados += len(resp_final.text.encode('utf-8'))
                    if resp_final.status_code != 200:
                        raise Exception(f"Falha ao acessar a URL final: {resp_final.status_code}")
                    soup_2 = BeautifulSoup(resp_final.text, "html.parser")
                    html_combinado = str(soup_1) + str(soup_2)
                    soup_combinado = BeautifulSoup(html_combinado, "html.parser")
                else:
                    soup_combinado = soup_1
            else:
                soup_combinado = soup_1

            movimentacoes = await extrair_movimentacoes(soup_combinado)
            results = {
                'code': 200,
                'message': 'SUCESSO',
                'datetime': datetime.now().isoformat(),
                'results': movimentacoes,
                'telemetria': telemetria
            }

    except Exception as e:
        logger.error(f"Erro durante a consulta: {e}")
        if telemetria.tentativas < TENTATIVAS_MAXIMAS_CAPTCHA:
            logger.info("Tentando resolver o CAPTCHA novamente...")
            telemetria.tentativas += 1
            return await fetch(numero_processo, telemetria)
        else:
            results = {
                'code': 4,
                'message': 'ERRO_SERVIDOR_INTERNO',
                'telemetria': telemetria
            }
    finally:
        telemetria.tempo_total = round(time.time() - inicio_tempo, 2)
        if isinstance(results, dict) and "telemetria" not in results:
            results["telemetria"] = telemetria
        return results
