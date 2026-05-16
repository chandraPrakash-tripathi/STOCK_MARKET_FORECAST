##connecting to postgresql
## creating engine using sql alchemy
## creating session factry using sql alchemy
##wrapping session in context manager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from scripts.config import DATABASE_URL
from contextlib import contextmanager
from loguru import logger

engine = create_engine(
    url=DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)
sessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_db():
    session = sessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error connecting DB: {e}")
        raise
    finally:
        session.close()


def test_conn() -> bool:
    try:
        with get_db() as db:
            db.execute(text("SELECT 1"))
        logger.info("Connected successfully")
        return True
    except Exception as e:
        logger.error("Not connected")
        return False


if __name__ == "__main__":
    test_conn()
