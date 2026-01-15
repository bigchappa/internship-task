import asyncio
from datetime import datetime, timedelta
from typing import Dict, List

from sqlalchemy.ext.asyncio import AsyncSession

from celery_app import celery_app
from db import async_session_maker, get_async_session
from queries import (get_not_rollbacked_deposit_amount,
                     get_not_rollbacked_transactions_count,
                     get_not_rollbacked_withdraw_amount,
                     get_registered_and_deposit_users_count,
                     get_registered_and_not_rollbacked_deposit_users_count,
                     get_registered_users_count, get_transactions_count)


@celery_app.task
def generate_transaction_analysis_task(weeks=52):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(get_transaction_analysis(weeks=weeks))
        return result
    finally:
        loop.close()


async def get_transaction_analysis(weeks=52) -> List[Dict]:
    async with async_session_maker() as session:
        today = datetime.now().date()
        dt_lt = today
        dt_gt = today - timedelta(weeks=weeks)
        results = []
        for _ in range(weeks):

            registered_users_count = await get_registered_users_count(session, dt_gt=dt_gt, dt_lt=dt_lt)
            registered_and_deposit_users_count = await get_registered_and_deposit_users_count(
                session, dt_gt=dt_gt, dt_lt=dt_lt
            )
            registered_and_not_rollbacked_deposit_users_count = (
                await get_registered_and_not_rollbacked_deposit_users_count(session, dt_gt=dt_gt, dt_lt=dt_lt)
            )
            not_rollbacked_deposit_amount = await get_not_rollbacked_deposit_amount(session, dt_gt=dt_gt, dt_lt=dt_lt)
            not_rollbacked_withdraw_amount = await get_not_rollbacked_withdraw_amount(session, dt_gt=dt_gt, dt_lt=dt_lt)
            transactions_count = await get_transactions_count(session, dt_gt=dt_gt, dt_lt=dt_lt)
            not_rollbacked_transactions_count = await get_not_rollbacked_transactions_count(
                session, dt_gt=dt_gt, dt_lt=dt_lt
            )

            if any(
                [
                    registered_users_count > 0,
                    registered_and_deposit_users_count > 0,
                    registered_and_not_rollbacked_deposit_users_count > 0,
                    not_rollbacked_deposit_amount > 0,
                    not_rollbacked_withdraw_amount > 0,
                    transactions_count > 0,
                    not_rollbacked_transactions_count > 0,
                ]
            ):
                results.append(
                    {
                        "start_date": dt_gt,
                        "end_date": dt_lt,
                        "registered_users_count": registered_users_count,
                        "registered_and_deposit_users_count": registered_and_deposit_users_count,
                        "registered_and_not_rollbacked_deposit_users_count": registered_and_not_rollbacked_deposit_users_count,
                        "not_rollbacked_deposit_amount": not_rollbacked_deposit_amount,
                        "not_rollbacked_withdraw_amount": not_rollbacked_withdraw_amount,
                        "transactions_count": transactions_count,
                        "not_rollbacked_transactions_count": not_rollbacked_transactions_count,
                    }
                )

            dt_gt -= timedelta(weeks=1)
            dt_lt -= timedelta(weeks=1)

        return results
