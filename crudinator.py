import aiosqlite
import dataclasses
import pathlib

import aiosqlite.cursor

from libs.data_models import BaseModel
from .row_factories import dict_factory

@dataclasses.dataclass
class CrudinatorRow(BaseModel):
    """
    Base class to represent a row to/from crudinator.
    All it does is make the rowid field.
    """
    rowid: int = None

class Crudinator:
    """
    Creates a table, ensures indexes, and provides basic CRUD operations based on a dataclass.
    The dataclass needs to subclass CrudinatorRow, which ensures the rowid field is present.

    This object can be subclassed to override the CRUD functions or add special queries for
    your applicaiton.

    A cursor should be provided, but we expect the row factory to be a dictionary.

    Cursors are not retrieved from within to allow the caller to control commits/rollback.
    
    TODO: We need some kind of 'any' object for queries
    """
    def __init__(self, dc: object) -> None:
        if not dataclasses.is_dataclass(dc):
            raise TypeError('Did not pass a dataclass to the Crudinator.')
        if not issubclass(dc.__class__, CrudinatorRow):
            raise TypeError('Crudinator dataclasses must be a subclass of CrudinatorRow.')
        else:
            self.dc = dc
    
    async def ensure_table(self, cur: aiosqlite.Cursor, pk: list[str] = None):
        table_name = self.dc.__class__.__name__.lower()
        column_names = []
        for f in dataclasses.fields(self.dc):
            if f.name == 'rowid':
                continue
            column_names.append(f.name)


        sql = f"CREATE TABLE IF NOT EXISTS {table_name}({', '.join(column_names)}"

        if pk:
            pk_part = f', PRIMARY KEY ({', '.join(pk)})'
            sql += f'{pk_part});'
        else:
            sql += ');'

        print(sql)
        await cur.execute(sql)

    async def ensure_index(self, cur: aiosqlite.Cursor, name: str, columns: list[str], unique: bool = False):
        if unique:
            sql = "CREATE UNIQUE INDEX"
        else:
            sql = "CREATE INDEX"
        
        column_names = ', '.join(columns)
        sql += f" IF NOT EXISTS {name} ON {self.dc.__class__.__name__.lower()} ({column_names});"

        print(sql)

        await cur.execute(sql)

    async def create(self, row: CrudinatorRow, cur: aiosqlite.Cursor):
        ignore = ['rowid']
        columns = []
        values = []
        for f in dataclasses.fields(row):
            if f.name in ignore:
                continue
            columns.append(f.name)
            values.append(row.__getattribute__(f.name))

        columns_qm = ', '.join(columns)
        values_qm = ', '.join(['?']*len(values))

        sql = f"INSERT INTO {self.dc.__class__.__name__.lower()} ({columns_qm}) VALUES ({values_qm}) RETURNING rowid;"
        args = tuple(values)
        print(sql, args)

        await cur.execute(sql, args)
        rowid_row: dict = await cur.fetchone()
        print(f'got rowid_row: {rowid_row}')
        rowid = rowid_row.get('rowid')
        row.rowid = rowid

    async def read_generator(self, cur: aiosqlite.Cursor, prototype: CrudinatorRow|None = None, limit: int = 0, offset: int = 0, order_by: str = None, direction: str = 'ASC'):
        """
        Returns an async generator for rows matching the prototype.  Use the string 'any' in fields to match anything.
        """
        sql = f"SELECT rowid, * FROM {self.dc.__class__.__name__.lower()}"
        args = []
        if prototype:
            prototype_parts = []
            ignore_keys = ['rowid']
            ignore_values = ['any']
            attention = []
            
            for f in dataclasses.fields(prototype):
                value = prototype.__getattribute__(f.name)
                if f.name in ignore_keys: continue
                if value  in ignore_values: continue
                attention.append(f.name)
                args.append(prototype.__getattribute__(f.name))
            for i, f in enumerate(attention):
                if i == 0:
                    prototype_parts.append(f' WHERE {f}=?')
                else:
                    prototype_parts.append(f' AND {f}=?')
            sql += ''.join(prototype_parts)
        
        if limit:
            sql += f' LIMIT {limit} OFFSET {offset}'
        
        if order_by:
            sql += f' ORDER BY {order_by} {direction}'

        sql += ';'

        print(sql, args)
        await cur.execute(sql, args)
        async for row in cur:
            row: dict
            dc = self.dc.__class__()
            dc.parse_from_dict(row)
            yield dc

    async def update(self, row: CrudinatorRow, cur: aiosqlite.Cursor):
        """
        Use row rowid field.
        """
        if row.rowid is None:
            print('Tried to update a row without rowid')
            return
        
        sql = f"UPDATE {self.dc.__class__.__name__.lower()} SET "

        ignores = ['rowid']
        expressions = []
        args = []
        for f in dataclasses.fields(row):
            if f.name in ignores:
                continue
            expressions.append(f'{f.name}=?')
            args.append(row.__getattribute__(f.name))
        sql += f'{','.join(expressions)}'

        sql += ' WHERE rowid = ?;'
        args.append(row.rowid)

        await cur.execute(sql, args)

    async def delete(self, row: CrudinatorRow, cur: aiosqlite.Cursor):
        if row.rowid is None:
            print('Tried to delete a row without rowid')
            return
        sql = f"DELETE FROM {self.dc.__class__.__name__.lower()} WHERE rowid = ?;"
        args = (row.rowid,)
        await cur.execute(sql, args)

    
        
