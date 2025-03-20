# Monitoramento Processual - Tribunal de Justiça do Paraná :robot:

## Descrição
API de automação para monitoramento de processos no Tribunal do Paraná (TJPR). A aplicação utiliza **FastAPI** para expor endpoints de consulta processual, integrando com **HTTPX** para navegação e captura de dados do site oficial do TJPR.


## Funcionalidades
- Consulta automática de processos no site do TJPR
- Captura de movimentações processuais, incluindo eventos, data/hora, descrição e documentos
- Resolução automática de CAPTCHA utilizando OCR
- Endpoint REST para integração com outras aplicações


## Tecnologias Utilizadas
- **FastAPI**: Framework para construção de APIs rápidas e performáticas
- **HTTPX**: Para as requisições http
- **BeautifulSoup**: Extração e parsing de dados HTML
- **Uvicorn**: Servidor ASGI para execução do FastAPI
- **Docker**: Containerização para facilidade de deployment


## Requisitos
- Python 3.9 ou superior
- Docker (opcional para rodar via container)


## Instalação e Execução Local


1. Clone o repositório:
- git clone https://github.com/reissbruno/monitoramento-processual-tjpr
- cd monitoramento-processual-tjpr


2. Instale as dependências:
- pip install -r requirements.txt


3. Execute o servidor:
- uvicorn server:app --host 0.0.0.0 --port 8000


4. Acesse a documentação da API no navegador:
- http://localhost:8000/docs


## Utilizando com Docker
1. Construa a imagem Docker:
- docker build -t monitoramento-processual-tjpr .


2. Rode o container:
- docker run -d -p 8000:8000 --name monitoramento-tjpr monitoramento-processual-tjpr


3. Acesse a API no navegador:
- http://localhost:8000/docs


## Variáveis de Ambiente
| ENV VAR | Descrição | Default |
| --------- | ---------- | --------- |
| `BOT_NAME` | Nome do bot. Útil caso houver mais de um container rodando. | `monitoramento-processual-tjpr` |
| `LOG_LEVEL` | Nível de log (DEBUG, INFO, WARNING, ERROR) | `INFO` |
| `TEMPO_LIMITE` | Tempo limite em segundos para carregamento de página. | `180` |
| `TENTATIVAS_MAXIMAS_CAPCAPTCHA` | Máximo de tentativas para resolver o CAPTCHA. | `30` |
| `TENTATIVAS_MAXIMAS_RECURSIVAS` | Máximo de tentativas recursivas para consulta. | `30` |



## Endpoints da API

### Consultar Processo
* Consulta movimentações de um processo específico no TJPR.
    - GET /api/monitoramento-processual-tjpr/consulta


### Parâmetros
* processo (string, obrigatório): Número do processo a ser consultado
    - Exemplo de Requisição
    - curl -X 'GET' \
        'http://localhost:8000/api/monitoramento-processual-tjpr/consulta?processo=12345678901234567890' \
        -H 'accept: application/json'



| HTTP CODE | `code` | Descrição |
| --------- | ------ | --------- |
| `200`     | 0      | Sucesso |
| `422`     | 7      | Não foi possível processar |
| `502`     | 5      | Bad Gateway |
| `512`     | 4      | Erro ao executar parse da página |
