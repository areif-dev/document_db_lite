import sqlite3 as sql
from typing import Dict, List, Any, Tuple, Iterable
import json


class InvalidRecordError(Exception):
    """ Indicates that a record object does not belong to a given Table """

    pass


class TableNotFoundError(Exception):
    """ Indicates that there is no table in the db that matches a given name """

    pass


class Table:

    valid_types = {"INTEGER": int, "REAL": float, "TEXT": str, "BLOB": bytes}

    def __init__(
        self,
        db_loc: str,
        table_name: str,
        fields_and_types: Dict[str, str] = {},
        subtables: Iterable[str] = [],
    ):
        """
        Table is used to map SQLite rows to complex Python dicts
        :param db_loc: The path to the .db file in storage
        :param table_name: The name of this Table in the database
        :param subtables: An unordered list of other Tables that this Table
            will store references to
        :param fields_and_types: Map the name of a column to its type. Must be
            one of (integer, real, text, blob). Int will map to python int, real
            to python float, text to python string, and blob to a byte string.
            Each table has at least one default field: id: int, which is the
            primary key for the table
        """

        # Testing that each field has a valid type. Either integer, text, real,
        # blob
        for field in fields_and_types:

            fields_and_types[field] = fields_and_types[field].upper()

            if fields_and_types[field] not in Table.valid_types:
                raise ValueError(
                    f"The type of an SQLite field must be one of {Table.valid_types}"
                )

        self.db_loc = db_loc
        self.table_name = table_name
        self._subtables = subtables
        self._fields = fields_and_types
        self._max_id = 0

        self._create_self()

    def __repr__(self) -> str:
        """ Gives the name of the table """
        return self.table_name

    def _create_self(self):
        """ Creates this Table in the database if it doesn't exit. Private """

        conn = sql.connect(self.db_loc)
        resp = conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()

        if len(resp) == 0:

            create_stmt = (
                "CREATE TABLE " + self.table_name + " (id INTEGER PRIMARY KEY NOT NULL"
            )

            # Add each field and its type to the creation statement
            for field in self._fields:
                create_stmt += ", " + field + " " + self._fields[field].upper()
            create_stmt += ", subrecords TEXT)"

            conn.execute(create_stmt)

            insert_str = f"INSERT into {self.table_name} (id, subrecords) VALUES (0, "
            if len(self._subtables) == 0:
                insert_str += "'{}')"

            else:
                insert_str += "'{\"" + self._subtables[0] + '": []'
                for table in self._subtables[1:]:
                    insert_str += f', "{table}": []'
                insert_str += "}')"

            conn.execute(insert_str)
            conn.commit()
        conn.close()

    @staticmethod
    def get_table(db_loc: str, table_name: str) -> "Table":
        """
        Creates a Table object from info in the db matching the given table_name

        :raises TableNotFoundError: If there is no table in db_loc with the name table_name
        :raises FileNotFoundError: If the db_loc file does not exist
        :param db_loc: The path to the database file to look through
        :param table_name: The name of the table to fetch from the db
        :return: A Table object with data matching that from the db
        """

        # Attempting to locate a db file. If none exists, raise an error. This
        # helps to avoid confusion created by just creating a new file in an
        # erroneous location
        try:
            with open(db_loc) as _:
                pass
        except FileNotFoundError:
            raise FileNotFoundError(f"No such file: {db_loc}")

        conn = sql.connect(db_loc)
        table_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()

        # Report error if the table name is not in the db
        if len(table_info) == 0:
            raise TableNotFoundError(f"{table_name} not in {db_loc}")

        else:

            # Collecting fields to assign to the Table
            fields = {}
            for field in table_info:
                if field[1] not in ("id", "subrecords"):
                    fields[field[1]] = field[2]

            subtables: List[str] = []

            # The 0th item in each table contains meta information about the
            # subtable names in each Table
            resp = conn.execute(
                f"SELECT subrecords FROM {table_name} WHERE id = 0"
            ).fetchone()
            conn.close()

            # There are no subtables for this Table
            if resp is None:
                return Table(db_loc, table_name, fields)

            subrecords_raw = json.loads(resp[0])

            # Recursively create subtables from the db
            for table_str in subrecords_raw:
                subtables.append(table_str)

            return Table(db_loc, table_name, fields, subtables)

    def get_next_id(self) -> int:
        """
        Gets the next id in the sequence from the db

        :return: The next id to be assigned to a new object
        """

        conn = sql.connect(self.db_loc)
        table_max_id = conn.execute(f"SELECT MAX(id) FROM {self.table_name}").fetchone()

        if table_max_id is None:
            max_id = self._max_id

        elif table_max_id[0] < self._max_id:
            res = conn.execute(
                f"SELECT id FROM {self.table_name} WHERE id = {self._max_id}"
            ).fetchone()

            if res is None:
                max_id = self._max_id
            else:
                self._max_id += 1
                return self.get_next_id()

        else:
            self._max_id = table_max_id[0]
            max_id = table_max_id[0]

        self._max_id += 1
        conn.close()
        return max_id + 1

    def get_fields(self) -> Dict[str, str]:
        """ Getter for the _fields attribute """
        return self._fields

    def fetchall(self) -> List[Dict[str, Any]]:
        """
        Gets all records belonging to this Table

        :return: A list of dictionaries containing all information this Table
            is responsible for
        """

        conn = sql.connect(self.db_loc)
        select_stmt = (
            "SELECT id, "
            + ", ".join(self._fields)
            + ", subrecords FROM "
            + self.table_name
        )
        resp = conn.execute(select_stmt).fetchall()
        conn.close()

        if len(resp) == 0:
            return []
        
        records = []
        for rec in resp:
            records.append({"id": rec[0]})

            for i, field in enumerate(self._fields, 1):
                records[-1][field] = rec[i]
            subrecords_raw = json.loads(rec[-1])
            subrecords = {}

            # Creating Table objects from raw subrecord string
            for table_name in subrecords_raw:

                subrecords[table_name] = []

                # Get the records for each subtable
                for i in subrecords_raw[table_name]:
                    subrecords[table_name].append(
                        Table.get_table(self.db_loc, table_name).get_record(i)
                    )

            records[-1]["subrecords"] = subrecords
        
        return records

    def get_record(self, id: int) -> Dict[str, Any]:
        """
        Creates a record object from the database with a given id

        :param id: The integer identifier of the record from this Table to fetch
        :return: The record dictionary created by fetching from the db
        """

        # Fetching raw data from the db
        conn = sql.connect(self.db_loc)
        select_stmt = (
            "SELECT id, "
            + ", ".join(self._fields)
            + ", subrecords FROM "
            + self.table_name
            + f" WHERE id = {id}"
        )
        resp = conn.execute(select_stmt).fetchone()
        conn.close()

        if resp is None:
            raise InvalidRecordError(
                "There is no record from " + self.table_name + f" with id = {id}"
            )

        # Adding fields and vals to the finished record
        record = {"id": resp[0]}
        for i, field in enumerate(self._fields, 1):
            record[field] = resp[i]
        subrecords_raw = json.loads(resp[-1])
        subrecords = {}

        # Creating Table objects from raw subrecord string
        for table_name in subrecords_raw:

            subrecords[table_name] = []

            # Get the records for each subtable
            for i in subrecords_raw[table_name]:
                subrecords[table_name].append(
                    Table.get_table(self.db_loc, table_name).get_record(i)
                )

        record["subrecords"] = subrecords
        return record

    def save_record(self, record: Dict[str, Any]):
        """
        Writes the data of a record dict to the database

        :raises InvalidRecordError: If the record given is not valid for this
            Table
        :param record: The record to write to this Table in db. Must be valid
        :param overwrite_id: Optional. If you want to overwrite another record
            already in the database, provide the id of that record here
        """

        # Raises exception if the given record is not valid for this Table
        resp = self.is_valid_record(record)
        if not resp[0]:
            raise InvalidRecordError(resp[1])

        if len(record["subrecords"]) == 0:
            subrecords_str = "'{}'"

        # Create a string representation of the subrecords field
        else:
            subrecords_str = "'{"
            table: str
            for table in record["subrecords"]:
                subrecords_str += '"' + table + '": ['

                # Storing references to each subrecord as the id of the record
                if len(record["subrecords"][table]) > 0:
                    subrecords_str += f"{record['subrecords'][table][0]['id']}"
                    for rec in record["subrecords"][table][1:]:
                        subrecords_str += f",{rec['id']}"

                subrecords_str = subrecords_str + "],"

            subrecords_str = subrecords_str[:-1] + "}'"

        conn = sql.connect(self.db_loc)

        save_stmt = ""

        # If the record is already saved, update the record
        if (
            conn.execute(
                "SELECT id FROM " + self.table_name + f" WHERE id = {record['id']}"
            ).fetchone()
            is not None
        ):

            save_stmt += "UPDATE " + self.table_name + " SET "

            for field in record:
                if field in ("id", "subrecords"):
                    continue

                # Aggregating fields into strings
                save_stmt += field + " = "
                if isinstance(record[field], str):
                    save_stmt += '"' + record[field] + '"'
                else:
                    save_stmt += str(record[field])
                save_stmt += ", "

            save_stmt += f"subrecords = {subrecords_str} WHERE id = {record['id']}"

        # If the record is not already saved, insert it in the db
        else:

            fields = []
            for field in record:
                if field not in ("id", "subrecords"):
                    fields.append(field)

            save_stmt += (
                "INSERT INTO "
                + self.table_name
                + " (id,"
                + ",".join(fields)
                + f",subrecords) VALUES ({record['id']}"
            )

            # Aggregating fields into strings
            for field in fields:
                save_stmt += ","
                if isinstance(record[field], str):
                    save_stmt += '"' + record[field] + '"'
                else:
                    save_stmt += str(record[field])

            save_stmt += f",{subrecords_str})"

        conn.execute(save_stmt)
        conn.commit()
        conn.close()

        for table in record["subrecords"]:
            for r in record["subrecords"][table]:
                Table.get_table(self.db_loc, table).save_record(r)

    def is_valid_record(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check that a given record is valid for this Table so no erroneous
        records are added

        :param record: The record to check. Form of {"id": int, **fields: any,
            "subrecords": {Table: [record]}}
        :return: Tuple with a boolean declaring whether the record is valid
            and/or a message detailing why it isn't valid
        """

        # Number of fields for the table must match number of fields in the
        # record
        if len(record) - 2 != len(self._fields):
            return (
                False,
                f"{self} requires {len(self._fields) + 2} fields. {len(record)} given.",
            )

        # Records must contain an id so they can be identified in the db
        if "id" not in record:
            return (False, 'Record must contain an "id" field.')

        # Records must have "subrecords" field even if they don't have
        # subrecords
        if "subrecords" not in record:
            return (False, 'Record must contain "subrecords" field.')

        # The number of tables in subrecords must match the number of tables in
        # subtables
        if len(self._subtables) != len(record["subrecords"]):
            return (
                False,
                f"{self} requires {len(self._subtables)} subtables defined. Received {len(record['subrecords'])}.",
            )

        for field in self._fields:

            # Each field in the Table must also be in the record
            if field not in record:
                return (
                    False,
                    f"{field} must be defined in records belonging to {self.table_name}",
                )

            # Each field must have the same type as the field in the Table
            if not isinstance(record[field], Table.valid_types[self._fields[field]]):
                return (
                    False,
                    f"{field} of type {type(record[field])} does not match expected {Table.valid_types[self._fields[field]]}.",
                )

        for table in record["subrecords"]:

            # Each table in subtables must also be in subrecords
            if table not in self._subtables:
                return (
                    False,
                    f"{table} is not defined as a subtable of {self.table_name}",
                )

            # Each record in subrecords must be valid or the whole thing is
            # invalid
            for rec in record["subrecords"][table]:
                resp = Table.get_table(self.db_loc, table).is_valid_record(rec)
                if not resp[0]:
                    return (False, resp[1])

        return (True, "Valid")

    def create_record(
        self, fields: Dict[str, Any], subrecords: Dict[str, List[Dict]] = {}
    ) -> Dict[str, Any]:
        """
        Used to format a collection of fields and subrecords with an unused id
        from the database.

        :raises InvalidRecordError: If the given information cannot create a valid
            record for this Table
        :param fields: {field_name: field_val} Dict that defines the fields and
            vals to include in the record
        :param subrecords: {Table: [record, record]} Any subobjects organized by
            their table to include in this record
        :return: A formatted record like {"id": 1, "field1": 1, "field2": "2",
            "subrecords": {OtherTable: [subrecord]}}
        """

        record = {"id": self.get_next_id()}
        for field in fields:
            record[field] = fields[field]
        record["subrecords"] = subrecords

        resp = self.is_valid_record(record)
        if not resp[0]:
            raise InvalidRecordError(resp[1])
        else:
            return record

    def delete_record(self, id: int):
        """
        Removes a record from the database. Only removes the specific record
            belonging to id, and does not recursively descend into subrecords

        :param id: The id number of the record to delete
        """

        conn = sql.connect(self.db_loc)
        conn.execute("DELETE FROM " + self.table_name + f" WHERE id = {id}")
        conn.commit()
        conn.close()

    def search_records(
        self,
        field_to_search: str,
        search_for: str,
        exclude_ids: Iterable[int] = (),
        strict=False,
    ) -> List[Dict[str, Any]]:
        """
        Gets a list of records from the db that have a given field that matches a given pattern

        :raises InvalidRecordError: When field_to_search is not defined in Table._fields
        :param field_to_search: The name of the field to base the search on
        :param search_for: The value to search for in the db
        :param exclude_ids: An array of ids to exclude from the search. Optional
        :param strict: Whether the search should look for exact matches or just
            similar matches. Default is False or relative search
        :return: A list of records that meet the search parameters
        """

        def _get_keywords(s: str) -> List[str]:
            """
            Breaks a string into keywords separated by spaces and keyphrases separated
            by quotes. Private

            :param s: The string to parse containing space separated keywords and quoted
                keyphrases that should retain their spaces
            :return: A list of phrases and words taken from s
            """

            # Clean up leading and trailing whitespace
            s = s.strip()

            # There are at least two quotes in s, so find and return the phrase
            if s[0] == '"' and s.count('"') > 1:
                phrase_end = s[1:].find('"') + 1
                return [s[1:phrase_end]] + _get_keywords(s[phrase_end + 1 :])

            # There are at least two keywords in the phrase, so find and break them up
            if " " in s:
                space_loc = s.find(" ")
                return [s[:space_loc]] + _get_keywords(s[space_loc + 1 :])
            return [s]

        # Take exception if the field_to_search is not a field defined in Table
        if field_to_search not in self._fields:
            raise InvalidRecordError(
                f"{field_to_search} is not a field in {self.table_name}"
            )

        else:

            if strict:
                search_str = (
                    "SELECT id FROM "
                    + self.table_name
                    + " WHERE "
                    + field_to_search
                    + " = "
                    + search_for
                )

            else:
                key_words = _get_keywords(search_for)

                search_str = (
                    "SELECT id FROM "
                    + self.table_name
                    + " WHERE ("
                    + field_to_search
                    + ' LIKE "%'
                    + key_words[0]
                    + '%"'
                )
                for key_word in key_words[1:]:
                    search_str += (
                        " OR " + field_to_search + ' LIKE "%' + key_word + '%"'
                    )
                search_str += (
                    " OR " + field_to_search + ' LIKE "%' + search_for + f'%")'
                )

            for id in exclude_ids:
                search_str += f" AND id != {id}"

            conn = sql.connect(self.db_loc)
            resp = conn.execute(search_str).fetchall()
            conn.close()

            records = []
            for i in resp:
                records.append(self.get_record(i[0]))

            return records


if __name__ == "__main__":

    Customer = Table.get_table("petclub.db", "Customer")
    for customer in Customer.fetchall():
        print(customer, end="\n\n")
