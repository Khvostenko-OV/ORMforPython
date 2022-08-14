import json
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from models import Publisher, Shop, Book, Stock, create_tables, models

# подключение к базе создание таблиц
with open('sql.txt') as file:
    sql_user = file.readline().strip()
    sql_psw = file.readline().strip()

DSN = "postgresql://" + sql_user + ":" + sql_psw + "@localhost:5432/books_db"
engine = sq.create_engine(DSN)
create_tables(engine)

# открытие сессии
Session = sessionmaker(bind=engine)
session = Session()

# заполнение таблиц данными
with open('tests_data.json') as file:
    data = json.load(file)

for record in data:
    model = models[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))

session.commit()

# запросы
pub = "."
while pub != "":
    pub = input("Введите издателя: ")
    if pub == "":
        break
    shops = session.query(Shop).join(Shop.stock, Stock.book, Book.publisher).filter(Publisher.name.ilike(f"%{pub}%")).all()
    # select sh.name from shop sh
    #   join stock st
    #   on sh.id = st.id_shop
    #   join book b
    #   on st.id_book = b.id
    #   join publisher p
    #   on b.id_publisher = p.id
    #   where p.name ilike '%{pub}%';
    if len(shops) == 0:
        print(f"Издатель '{pub}' в торговой сети не найден")
    else:
        print(f"Издатель '{pub}' представлен в магазинах:")
        for sh in shops:
            print(sh.name)

session.close()
