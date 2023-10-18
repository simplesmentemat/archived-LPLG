import asyncio
from module.parse import *

async def main():
    schedule_data = get_schedule_data()
    print(schedule_data)
if __name__ == "__main__":
    asyncio.run(main())
