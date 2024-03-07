# DataBaseEngine

O é uma solução flexível em Python projetada para facilitar a conexão e operações com bancos de dados MySQL e PostgreSQL. Este repositório oferece duas abordagens distintas: uma orientada a objetos, ideal para aplicações complexas e integração profunda, e uma abordagem funcional, perfeita para scripts rápidos e tarefas de manipulação de dados pontuais.

### Características

- Abordagem Orientada a Objetos: Encapsula a lógica de conexão e operações de banco de dados em uma classe, facilitando a gestão de estados e a reutilização.
- Abordagem Funcional: Fornece funções modulares para operações específicas de banco de dados, oferecendo simplicidade e flexibilidade.
- Suporte para MySQL e PostgreSQL.
- Integração fácil com pandas para operações de inserção em lote a partir de DataFrames.
- Configuração dinâmica baseada em variáveis de ambiente para diferentes ambientes (desenvolvimento, homologação, produção).

### Requisitos

- pymysql
- psycopg2
- pandas
- airflow (opcional, para a abordagem orientada a objetos)

### Instalação

```bash
pip install pymysql psycopg2 tqdm pandas apache-airflow
```

### Uso

Abordagem Orientada a Objetos (DataBaseEngine.py)

```python
from database_engine import DataBaseEngine
import pandas as pd

df = pd.DataFrame({'coluna1': [1, 2], 'coluna2': ['A', 'B']})

engine = DataBaseEngine(bd_type='mysql', database='nome_do_banco', environment='dev', schema='public')
engine.connection()
engine.batch_insert(df)
engine.dispose()
```

Abordagem Funcional (DataBaseEngine_script.py)

```python
import pandas as pd
from seu_modulo import batch_insert  # Substitua 'seu_modulo' pelo nome do arquivo

db_config = {
    "host": "host_do_banco",
    "user": "user_do_banco",
    "password": "senha_do_banco",
    "database": "nome_do_banco"
}

df = pd.DataFrame({'coluna1': [1, 2], 'coluna2': ['A', 'B']})

batch_insert(df, pymysql, 'mysql', db_config, 'sua_tabela')
```

Métodos e Funções

- Orientada a Objetos:

  - connection()
  - dispose()
  - batch_insert(df, batch=0)

- Funcional:
  - create_conn(module, db_config)
  - sql_for_insert(cur, table, bd_type)
  - batch_insert(df, module, bd_type, db_config, table, schema='public')

Licença

MIT License - Veja o arquivo LICENSE para mais detalhes.

Autor

- Arthur Nemi Guilarde

---
