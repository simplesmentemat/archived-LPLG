# LPLG

## Overview

### PTBR

A biblioteca LPLGather é uma ferramenta poderosa que coleta e organiza dados da Liga LPL em um formato conveniente de CSV. Com esta biblioteca, você pode facilmente extrair informações atualizadas e estatísticas da liga para análises, visualizações de dados ou qualquer outro fim que você possa precisar. Simplifique a obtenção de dados da LPL e aumente a eficiência do seu projeto com o LPLGather.

## Instalação

Para instalar basta seguir o comando:

```bash
pip install LPLG
```

### ENG

The LPLGather library is a powerful tool that gathers and organizes data from the LPL in a convenient CSV format. With this library, you can easily extract up-to-date information and league statistics for analysis, data visualizations, or any other purpose you may need. Simplify the acquisition of LPL data and enhance the efficiency of your project with LPLGather.

## Installation

To install LPLGather, use the following command:

```bash
pip install LPLG
```

# DOCS

[Documentação](https://matpaulods.github.io/LPLG/)

### .ENV
```bash
API_HEADERS_AUTH="xxx"
```
Exemple:
```py
from LPLGather import parse
df = parse.get_match_data(190,1)
print(df)
```

### Agradecimentos

Agradeço a rMaze pelo codigo do champion map.