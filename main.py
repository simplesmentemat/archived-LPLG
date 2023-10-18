import asyncio
from module.parse import *

async def main():
    player_df = processar_dados_lol(190,1)
    
if __name__ == "__main__":
    asyncio.run(main())
