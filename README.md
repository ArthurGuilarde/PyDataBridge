# Gerenciador de Banco de Dados para MySQL e PostgreSQL

Este módulo fornece uma interface simplificada para a gestão de conexões com bancos de dados MySQL e PostgreSQL, facilitando a realização de operações de CRUD (Create, Read, Update, Delete), além de permitir testes de conexão e o fechamento seguro das conexões.

## Características

- Conexão simplificada com MySQL e PostgreSQL.
- Geração dinâmica de strings SQL para operações de inserção e "upsert".
- Teste de conexão para validar configurações.
- Encerramento seguro de conexões com o banco de dados.
- Suporte para variáveis de ambiente para configuração segura.

## Dependências

Este módulo depende de várias bibliotecas externas para funcionar corretamente. Certifique-se de ter as seguintes dependências instaladas:

- `os`
- `logging`
- `hashlib`
- `pymysql`
- `psycopg2`
- `pandas`
- `datetime`
- `tqdm`
- `python-dotenv`

Você pode instalar todas as dependências necessárias com o seguinte comando:

```bash
pip install pymysql psycopg2 pandas tqdm python-dotenv
```

## Configuração

Para utilizar o GerenciadorBancoDados, você precisa configurar as variáveis de ambiente relacionadas à conexão com o banco de dados. Um exemplo de configuração pode ser encontrado no arquivo .env.example neste repositório. Copie este arquivo para um arquivo .env e preencha com os seus dados de conexão.
<br><br>
Exemplo de .env:

```.env
DATABASE_URL=seu_host_do_banco
DATABASE_USER=seu_usuario
DATABASE_PASS=sua_senha
```

## Uso

Aqui está um exemplo rápido de como utilizar o GerenciadorBancoDados para conectar a um banco de dados MySQL, testar a conexão e encerrar a conexão:

```python
from gerenciador_banco_dados import GerenciadorBancoDados

# Inicializa o gerenciador para um banco de dados específico e tabela
gerenciador = GerenciadorBancoDados(database="nome_do_banco", table="nome_da_tabela", bd_type='mysql')

# Testa a conexão
gerenciador.conection_test()

# Conecta ao banco de dados
gerenciador.connect()

# Encerra a conexão
gerenciador.dispose()
```

## Contribuindo

Contribuições para o módulo são bem-vindas. Para contribuir, por favor, abra uma issue ou um pull request com suas sugestões ou correções.

## Licença

Este projeto é distribuído sob a licença MIT. Veja o arquivo LICENSE para mais detalhes.
