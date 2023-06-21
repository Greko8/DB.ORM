import sqlalchemy
import json
from pprint import pprint
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale


login = 'postgres'
password = 'password1'
database = 'books'

DSN = f"postgresql://{login}:{password}@localhost:5432/{database}"
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('test_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

Name_id = input('Введите ID автора базе данных:')

query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Sale.stock).join(Stock.shop).join(Stock.book).join(Book.publisher).filter(Publisher.id == Name_id).all()
for c in query:
    print(' | '.join(map(str, c)))
