import datetime
from datetime import timedelta

import uvicorn
from celery.result import AsyncResult
from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)
from sqlalchemy.orm import selectinload

from celery_app import celery_app
from db import create_db_and_tables, delete_db_and_tables, get_async_session
from db_models import *
from exceptions import (BadRequestDataException,
                        CreateTransactionForBlockedUserException,
                        NegativeBalanceException,
                        TransactionAlreadyRollbackedException,
                        TransactionDoesNotBelongToUserException,
                        TransactionNotExistsException,
                        UpdateTransactionForBlockedUserException,
                        UserAlreadyActiveException,
                        UserAlreadyBlockedException,
                        UserAlreadyExistsException, UserAlreadyHasThisStatus,
                        UserNotExistsException)
from python_models import *
from tasks import generate_transaction_analysis_task

app = FastAPI()


@app.on_event("startup")
async def on_startup(session: AsyncSession = Depends(get_async_session)):
    await create_db_and_tables()


@app.get("/users", response_model=list[ResponseUserModel])
async def get_users(session: AsyncSession = Depends(get_async_session), offset: int = 0, limit: int = 100):
    q = select(User).options(selectinload(User.user_balance)).order_by(User.created.asc()).offset(offset).limit(limit)
    result = await session.execute(q)
    users = result.scalars().all()
    return users


@app.post("/users", response_model=ResponseUserModel)
async def post_user(user: RequestUserModel, session: AsyncSession = Depends(get_async_session)):
    db_user = await session.execute(select(User).where(User.email == user.email))
    if db_user.scalar_one_or_none():
        raise UserAlreadyExistsException(
            status_code=status.HTTP_409_CONFLICT, detail=f"User with email=`{user.email}` already exists"
        )
    date_time_now = datetime.now()
    db_user = User(email=user.email, status="ACTIVE", created=date_time_now)
    session.add(db_user)
    await session.flush()
    user_balances = [
        UserBalance(user_id=db_user.id, currency=currency, amount=0, created=date_time_now) for currency in CurrencyEnum
    ]
    session.add_all(user_balances)
    await session.commit()
    result = await session.execute(select(User).where(User.id == db_user.id).options(selectinload(User.user_balance)))
    return result.scalar_one_or_none()


@app.patch("/users/{user_id}", response_model=UserModel)
async def patch_user(user_id: int, user: RequestUserUpdateModel, session: AsyncSession = Depends(get_async_session)):
    result = await session.execute(select(User).where(User.id == user_id))
    db_user = result.scalar_one_or_none()
    if not db_user:
        raise UserNotExistsException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with id=`{user_id}` does not exist"
        )
    if db_user.status == user.status:
        raise UserAlreadyHasThisStatus(status_code=status.HTTP_400_BAD_REQUEST, detail=f"User is already {user.status}")

    db_user.status = user.status
    session.add(db_user)
    await session.commit()
    await session.refresh(db_user)
    return db_user


@app.get("/transactions", response_model=list[TransactionModel])
async def get_transactions(
    user_id: Optional[int] = None, session: AsyncSession = Depends(get_async_session)
) -> List[TransactionModel]:
    q = select(Transaction).order_by(Transaction.created.desc())
    if user_id:
        q = q.where(Transaction.user_id == user_id)

    result = await session.execute(q)
    transactions = result.scalars().all()

    return transactions


@app.post("/{user_id}/transactions", response_model=TransactionModel)
async def post_transaction(
    user_id: int, transaction: RequestTransactionModel, session: AsyncSession = Depends(get_async_session)
):

    user = await session.get(User, user_id)
    if not user:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User with id {user_id} does not exist")
    if user.status != "ACTIVE":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"User with id {user_id} is blocked")

    user_balance_q = select(UserBalance).where(
        UserBalance.user_id == user_id, UserBalance.currency == transaction.currency.value
    )
    user_balance_result = await session.execute(user_balance_q)
    user_balance = user_balance_result.scalar_one_or_none()
    if not user_balance:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User does not have balance in {transaction.currency.value}"
        )
    new_balance = float(user_balance.amount) + transaction.amount
    if new_balance < 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Negative balance")
    user_balance.amount = new_balance
    session.add(user_balance)
    db_transaction = Transaction(
        user_id=user_id,
        currency=transaction.currency.value,
        amount=transaction.amount,
        status="PROCESSED",
        created=datetime.now(),
    )
    session.add(db_transaction)
    await session.commit()
    await session.refresh(db_transaction)

    return db_transaction


@app.patch("/{user_id}/transactions/{transaction_id}", response_model=TransactionModel)
async def patch_rollback_transaction(
    user_id: int, transaction_id: int, session: AsyncSession = Depends(get_async_session)
):
    async with session.begin():
        user = await session.get(User, user_id)
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User with id {user_id} does not exist")
        if user.status == "BLOCKED":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"User with id {user_id} is BLOCKED")

        transaction_q = select(Transaction).where(Transaction.id == transaction_id).with_for_update()
        result = await session.execute(transaction_q)
        transaction = result.scalar_one_or_none()
        if not transaction:
            raise TransactionNotExistsException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Transaction with id=`{transaction_id}` does not exist"
            )
        if transaction.user_id != user_id:
            raise TransactionDoesNotBelongToUserException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Transaction with id=`{transaction.id}` does not belong to user with id=`{user.id}`",
            )
        if transaction.status == "ROLLBACKED":
            raise TransactionAlreadyRollbackedException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Transaction with id=`{transaction.id}` is already rollbacked",
            )

        balance_q = (
            select(UserBalance)
            .where(UserBalance.user_id == user_id, UserBalance.currency == transaction.currency)
            .with_for_update()
        )
        balance_result = await session.execute(balance_q)
        user_balance = balance_result.scalar_one_or_none()

        if not user_balance:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Balance not found")

        new_balance = user_balance.amount - transaction.amount

        if new_balance < 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Balance can not be negative")

        user_balance.amount = new_balance
        transaction.status = "ROLLBACKED"

    return transaction


@app.get("/transactions/analysis", response_model=dict)
async def get_transaction_analysis(weeks: int = 52, session: AsyncSession = Depends(get_async_session)) -> List[dict]:
    task = generate_transaction_analysis_task.delay()
    return {"task_id": task.id, "status": "processing"}


@app.get("/tasks/{task_id}/result")
async def get_task_result(task_id: str):
    task = AsyncResult(task_id, app=celery_app)

    if not task.ready():
        raise HTTPException(status_code=404, detail="Task not completed yet")

    if task.failed():
        raise HTTPException(status_code=500, detail=f"Task failed: {task.info}")

    return task.result


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=7999, reload=True)
