"""main module"""

import sqlite3
import os
import datetime
import yaml
import regex as re
import pandas as pd
import unidecode
import os


class Pipeline:
    def __init__(self, db_path):
        self.db_path = db_path

    # Formatting functions, more or less helper functions
    @staticmethod
    def _field_name_to_db_format(column_name):
        """
        transforms the string passed into a sequence of alphanumeric characters in lower case
        any non alphanumeric characters will be deleted, the alphanumeric numeric characters will then
        be joined with underscores

            Parameters:
                :param column_name: The string to format -> str

            Returns:
                if input: '//:ZErt88//:fdgg__Xkf'
                output: 'zert88_fdgg_xkf'
        """
        pattern = '[_\W]'
        l = re.split(pattern, unidecode.unidecode(column_name.lower()))
        string_to_return_list = []
        for element in l:
            if element != '':
                string_to_return_list.append(element)
        string_to_return = '_'.join(string_to_return_list)
        return string_to_return

    def _field_split(self, source_file, column_name_list, df, splitters_list, table_split_name, column_split_rename='',
                     id_column=''):
        """
        Splits the column of a dataframe such as: row_label:(a, b, c)
        into a new dataframe made of a column looking like :    row_label:a
                                                                row_label:b
                                                                row_label:c
        It also creates a table in the database given as an attribute during the instantiation of the class,
        :param source_file: The source_file that's gonna be written in the control_table -> str
        :param column_name_list: The columns that will be split as the example above -> list
        :param df: The pandas.DataFrame object on which the split will occur -> pandas.DataFrame oject
        :param splitters_list: The splitters that will be used to split the columns given in column_name_list -> list
        :param table_split_name: The name of the table that will be created -> str
        :param column_split_rename: The name of the columns that will be split, useful especially if you want to make
        more self-explanatory joins between two tables -> list
        :param id_column: adds a column to refer to of the dataframe that's being split,
        necessary for the joins ! -> str
        :return:
        Given the following Dataframe:
                             _____________________________________
            Dataframe A:    |perimeter_id      |      col_to_split|
                            |__________________|__________________|
                            |row_a             |      a, b, c     |
                            |__________________|__________________|

            DataFrame B = _field_split(source_file=...., column_name_list=['col_to_split'], df=DataFrame A,
            spliters_list=[','], table_split_name=....., column_split_rename=['peri_level_0_id'],
            id_column=['perimeter_id']

                             _____________________________________
            Dataframe B:    |perimeter_id      |   peri_level_0_id|
                            |__________________|__________________|
                            |row_a             |         a        |
                            |__________________|__________________|
                            |row_a             |         b        |
                            |__________________|__________________|
                            |row_a             |         c        |
                            |__________________|__________________|
        """
        con = sqlite3.connect(self.db_path)
        for col_name, splitter, col_id, split_rename in zip(column_name_list, splitters_list, id_column,
                                                            column_split_rename):
            df_2 = df[[col_name]].copy()
            self._insert_control_columns_to_df(df_2, table_split_name)
            if col_id == '':
                df_2.insert(1, "Id", range(1, len(df_2) + 1))
            else:
                df_2.insert(1, col_id, df[col_id])
            df_2[col_name] = df_2[col_name].str.split(str(splitter))
            df_2 = df_2.explode(col_name).reset_index(drop=True)
            self._create_control_table(source_file, table_split_name)
            if column_split_rename != '':
                df_2.rename(columns={col_name: split_rename}, inplace=True)
                df_2.to_sql(table_split_name, con, index=False, if_exists='append')

    def _insert_control_columns_to_df(self, df, table_name):
        """
        insert a control column to the df in order to identify it with its primary key in the control_table,
        allows to join with the control_table
        :param df: the dataframe to which you 'll insert the column -> pandas.DataFrame Object
        :param table_name: string to check if the table_name exists -> str
        :return
            input:
                             _____________________________________
            Dataframe A:    |Col 1             |   Col 2          |
                            |__________________|__________________|
                            |row_a             |         a        |
                            |__________________|__________________|
                            |row_b             |         b        |
                            |__________________|__________________|
                            |row_c             |         c        |
                            |__________________|__________________|

            output:
                             __________________________________________________
            Dataframe A:    | Column id  |Col 1             |      Col 2       |
                            |____________|__________________|__________________|
                            |    id      |row_a             |         a        |
                            |____________|__________________|__________________|
                            |    id      |row_b             |         b        |
                            |____________|__________________|__________________|
                            |    id      |row_c             |         c        |
                            |____________|__________________|__________________|


        """
        if self._check_if_table_exists(table_name):
            max_upload = self._get_latest_upload(table_name)
            df.insert(0, "control_id", table_name + str(max_upload))
        else:
            df.insert(0, "control_id", table_name + '1')

    def _create_control_table(self, source_file, table_name):
        """
        creates the control table where all the control values are stored,
        values such as: the control id to be able to join with any table in the database
                        the source file
                        the upload id, that gives some kind of "version" to the table
                        the insert date column to store when the table was created
                        the user id to trace the table creator
        :param source_file: the file from where you extracted the datas, excelpath, dataframe name, ...
        :param table_name: the name of the table that's being inserted
        :return
                             _______________________________________________________________________________
            Control table:  | Control_id |source_file      |      upload      |   insert_date   |  user_id  |
                            |____________|_________________|__________________|_________________|___________|
                            |    tableA1 |excel A          |         1        |     12/01       | A455678   |
                            |____________|_________________|__________________|_________________|___________|
                            |    tableA2 |excel A          |         2        |     15/02       | A455678   |
                            |____________|_________________|__________________|_________________|___________|
                            |    tableB1 |excel B          |         1        |      02/03      | A789004   |
                            |____________|_________________|__________________|_________________|___________|
        """
        df_2 = pd.DataFrame()
        con = sqlite3.connect(self.db_path)
        if self._check_if_table_exists(table_name):
            max_upload = self._get_latest_upload(table_name) + 1
            df_2["control_id"] = [table_name + str(max_upload)]
            df_2["source_file"] = [source_file]
            df_2["upload"] = [max_upload]
            df_2["insert_date"] = [datetime.datetime.now()]
            df_2["user_id"] = [os.getlogin()]
        else:
            df_2["control_id"] = [table_name + '1']
            df_2["source_file"] = [source_file]
            df_2["upload"] = [1]
            df_2["insert_date"] = [datetime.datetime.now()]
            df_2["user_id"] = [os.getlogin()]
        df_2.to_sql('control_table', con, dtype={'control_id': 'PRIMARY KEY'}, index=False, if_exists='append')
        con.close()

    def _get_control_id(self, table_name):
        """
        get the control_id of the table name passed
        :param table_name -> str
        """
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("SELECT control_id from " + table_name)
        return cur.fetchone()[0]

    def _get_latest_upload(self, table_name):
        """
        get the latest upload value of the table name passed
        :param table_name: the name of the table in the database -> str
        :return: 
            if the last upload of the table 'table A' is 8 then returns 8.
        """
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("SELECT max(upload) FROM control_table WHERE control_id LIKE '" + table_name + "%'")
        max_upload = cur.fetchone()[0]
        return max_upload

    def _check_if_table_exists(self, table_name):
        """
        checks if the table with the nam passed exists in the database
        :param table_name: the name of the table in the database -> str
        :return: 
            if exists, returns 1
            else returns 0
        """
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute('CREATE TABLE temp.temporary_table(name);')
        cur.execute(
            '''INSERT INTO temp.temporary_table(name) SELECT count(name)
            FROM sqlite_master WHERE type='table'
            AND name='%s';''' % table_name)
        cur.execute('SELECT * FROM temporary_table;')
        val = cur.fetchone()[0]
        con.close()
        if val == 1:
            return True
        else:
            return False

    def _get_max_of_upload_ids(self, l):
        """
        returns the maximum upload id of a list of table_name after it was ordered
        :param l: the list you wanna order and get the maximum of -> list
        :return:
            l = [table_a, table_b, table_c, table_d]
            given the following upload_id values:
                upload_ids_dict = {table_a:1, table_b:1, table_c:3, table_d:6}
            then the method returns 6
        """
        if isinstance(l, list):
            order_list = [self._get_latest_upload(item) for item in l]
            return max(order_list)

    def _get_order_upload_dict(self, l):
        """
        gets the upload ids for each table_name in the list passed
        :param l: the list you want to find the upload_ids of -> list
        :return:
            l = [table_a, table_b, table_c, table_d]
            given that table_a has an upload_id of 1, table_b an upload id of1, 3 for table_c and 6 for table_d
            it returns {table_a:1, table_b:1, table_c:3, table_d:6}
        """
        if isinstance(l, list):
            order_dict = {item: self._get_latest_upload(item) for item in l}
            return order_dict

    def _update_upload_ids(self, table_name_list):
        """
        When inserting a group of tables via pipeline, you might want to even their upload_ids in order to join/ insert
        them with more ease when needed, this is what the method does
        :param table_name_list: a list of the table_name you want to insert together
        :return:
        let's say you have the following upload_ids for a given list of table:
            upload_ids_dict = {table_a:1, table_b:1, table_c:3, table_d:6}
        the upload_ids will be updated to:
            upload_ids_dict = {table_a:7, table_b:7, table_c:7, table_d:7}
        all the tables are then inserted and their upload ids will be 7.

        """
        if isinstance(table_name_list, list):
            maximum = self._get_max_of_upload_ids(table_name_list)
            print(maximum)
            for table_name in table_name_list:
                con = sqlite3.connect(self.db_path)
                latest_upload = self._get_latest_upload(table_name)
                print(latest_upload)
                print(table_name)
                table_latest_upload = table_name + str(latest_upload)
                new_table_latest = table_name + str(maximum + 1)
                print(new_table_latest)
                cur = con.cursor()
                cur.execute(
                    '''CREATE TABLE temp.tabl AS SELECT * FROM %s
                    WHERE control_id = '%s';''' % (table_name, table_latest_upload)
                )
                cur.execute(
                    '''UPDATE tabl SET control_id = '%s';''' % new_table_latest
                )
                cur.execute(
                    '''INSERT INTO %s SELECT * FROM tabl;''' % table_name
                )
                cur.execute(
                    '''SELECT * FROM %s WHERE control_id = '%s';''' % (table_name, new_table_latest)
                )
                cur.execute(
                    '''SELECT source_file FROM control_table WHERE control_id = '%s';''' % (table_latest_upload)
                )
                source_file = cur.fetchone()[0]
                con.commit()
                df_2 = pd.DataFrame()
                df_2["control_id"] = [new_table_latest]
                df_2["source_file"] = [source_file]
                df_2["upload"] = [maximum + 1]
                df_2["insert_date"] = [datetime.datetime.now()]
                df_2["user_id"] = [os.getlogin()]
                df_2.to_sql('control_table', con, dtype={'control_id': 'PRIMARY KEY'}, index=False, if_exists='append')
                con.close()

    @staticmethod
    def get_extension_from_file(file):
        extension = os.path.splitext(file)[1]
        return extension
    @staticmethod
    def get_max_len_header(header_dict):
        maks = max(header_dict, key=lambda k: len(header_dict[k]))
        return maks

    def insert_DataFrame_to_sqlite_table(
            self,
            df,
            table_name,
            source,
            table_split_name='',
            list_col_to_split='',
            list_splitters='',
            col_control_id='',
            list_column_split_rename=''
    ):
        """
        inserts the dataframe given to the database, when doing so their will be:
            - a check to see if the table exists or not
                if it does exists it will be updated
                if it doesnt it will be created
            - a control table is created/updated with different values for each tables
            - the column names of the dataframe will be formatted into a standard format
            - if a 'split' is needed, its arguments are passed
        :param df: the dataframe that will be inserted as a table in the database -> pandas.DataFrame object
        :param table_name: the name given to the table created via the dataframe passed above -> str
        :param source: the source of the dataframe, where its datas come from, excel path, csv_path, dataframe name, ...
        :param table_split_name: the name of the new table if their is a split (see field_split_method)
        :param list_col_to_split: the list of column that will be split (see field_split_method)
        :param list_splitters: the splitters used to split (see field_split_method)
        :param col_control_id: the id_column to add to the split table (see field_split_method)
        :param list_column_split_rename: how to rename the column once it was split (see field_split_method)
        """
        con = sqlite3.connect(self.db_path)
        df.columns = [self._field_name_to_db_format(item)
                      for item in list(df)]
        if self._check_if_table_exists(table_name):
            self._create_control_table(source, table_name)
            self._insert_control_columns_to_df(df, table_name)
            if list_col_to_split != '':
                self._field_split(source,
                                 list_col_to_split,
                                 df,
                                 list_splitters,
                                 table_split_name,
                                 list_column_split_rename,
                                 col_control_id)
            df.to_sql(table_name, con, index=False, if_exists='append')
        else:
            print('la table n existe pas, creation de la table')
            self._create_control_table(source, table_name)
            self._insert_control_columns_to_df(df, table_name)
            if list_col_to_split != '':
                self._field_split(source,
                                 list_col_to_split,
                                 df,
                                 list_splitters,
                                 table_split_name,
                                 list_column_split_rename,
                                 col_control_id)
            df.to_sql(table_name, con, index=False, if_exists='append')

    def insert_excel_data_to_sqlite_table(self,
                                          yaml_file='',
                                          _excel_path='',
                                          _sheet_name='',
                                          _list_column_rename='',
                                          _table_name='',
                                          _table_split_name='',
                                          _list_col_to_split='',
                                          _list_splitters='',
                                          _col_control_id='',
                                          _list_column_split_rename='',
                                          _skiprows=''
                                          ):
        """
        Insert an excel table in the database
        :param yaml_file: the path of a yaml file containing the args below -> str
        :param _excel_path: the path of the excel file -> str
        :param _sheet_name: the sheet of the excel file -> str
        :param _list_column_rename: if you want to modify the original column names of the excel -> list
        :param _table_name: the name of the excel once it's a table in the database
        :param _table_split_name: the name of the new table if their is a split (see field_split_method)
        :param _list_col_to_split: the list of column that will be split (see field_split_method)
        :param _list_splitters: the splitters used to split (see field_split_method)
        :param _col_control_id: the id_column to add to the split table (see field_split_method)
        :param _list_column_split_rename: how to rename the column once it was split (see field_split_method)
        :param _skiprows: if the table on the sheet is not isolated, you can ignore certain rows 
        -> list-like, int, or callable
        :return:
        """
        if yaml_file != '':
            yaml_file = open(yaml_file)
            yaml_content = yaml.load(yaml_file)
            yaml_dict = yaml_content['Pipeline_dict']
            excel_path = yaml_dict['excel_path']
            sheet_name = yaml_dict['sheet_name']
            list_column_rename = yaml_dict['list_column_rename']
            list_col_to_split = yaml_dict['list_col_to_split']
            list_splitters = yaml_dict['list_splitters']
            col_control_id = yaml_dict['col_control_id']
            table_name = yaml_dict['table_name']
            table_split_name = yaml_dict['table_split_name']
            list_column_split_rename = yaml_dict['list_column_split_rename']
            skiprows = yaml_dict['skiprows']
            for i in range(len(sheet_name)):
                table_name_ = table_name[i]
                table_split_name_ = table_split_name[i]
                sheet_name_ = sheet_name[i]
                excel_path_ = excel_path[i]
                list_column_rename_ = list_column_rename[i]
                list_col_to_split_ = list_col_to_split[i]
                list_splitters_ = list_splitters[i]
                col_control_id_ = col_control_id[i]
                list_column_split_rename_ = list_column_split_rename[i]
                skiprows_ = skiprows[i]
                con = sqlite3.connect(self.db_path)
                if list_column_rename_ != '':
                    df = pd.read_excel(excel_path_, sheet_name_, names=list_column_rename_, skiprows=skiprows_)
                else:
                    df = pd.read_excel(excel_path_, sheet_name_, skiprows=skiprows_)
                self.insert_DataFrame_to_sqlite_table(
                    df,
                    table_name_,
                    excel_path_,
                    table_split_name_,
                    list_col_to_split_,
                    list_splitters_,
                    col_control_id_,
                    list_column_split_rename_
                )
                con.close()
        if _excel_path != '':
            con = sqlite3.connect(self.db_path)
            if _sheet_name != '':
                if _list_column_rename != '':
                    df = pd.read_excel(_excel_path, _sheet_name, names=_list_column_rename, skiprows=_skiprows)
                else:
                    df = pd.read_excel(_excel_path, _sheet_name, skiprows=_skiprows)
            else:
                if _list_column_rename != '':
                    df = pd.read_excel(_excel_path, names=_list_column_rename, skiprows=_skiprows)
                else:
                    df = pd.read_excel(_excel_path, skiprows=_skiprows)
            self.insert_DataFrame_to_sqlite_table(
                df,
                _table_name,
                _excel_path,
                _table_split_name,
                _list_col_to_split,
                _list_splitters,
                _col_control_id,
                _list_column_split_rename
            )
            con.close()

    def insert_csv_data_to_sqlite_table(self,
                                        yaml_file='',
                                        _csv_path='',
                                        _table_name='',
                                        _list_column_rename='',
                                        _table_split_name='',
                                        _list_col_to_split='',
                                        _list_splitters='',
                                        _col_control_id='',
                                        _list_column_split_rename=''
                                        ):
        """
        insert a csv file to the database
        :param yaml_file: the path of a yaml file containing the args below -> str
        :param _csv_path: the path of the csv file -> str
        :param _table_name: the name of the excel once it's a table in the database
        :param _list_column_rename: if you want to modify the original column names of the excel -> list
        :param _table_name: the name of the excel once it's a table in the database
        :param _table_split_name: the name of the new table if their is a split (see field_split_method)
        :param _list_col_to_split: the list of column that will be split (see field_split_method)
        :param _list_splitters: the splitters used to split (see field_split_method)
        :param _col_control_id: the id_column to add to the split table (see field_split_method)
        :param _list_column_split_rename: how to rename the column once it was split (see field_split_method)
        :return: 
        """
        if yaml_file != '':
            yaml_file = open(yaml_file)
            yaml_content = yaml.load(yaml_file)
            yaml_dict = yaml_content['Pipeline_dict']
            csv_path = yaml_dict['csv_path']
            list_col_to_split = yaml_dict['list_col_to_split']
            list_column_rename = yaml_dict['list_column_rename']
            list_splitters = yaml_dict['list_splitters']
            col_control_id = yaml_dict['col_control_id']
            table_name = yaml_dict['table_name']
            table_split_name = yaml_dict['table_split_name']
            list_column_split_rename = yaml_dict['list_column_split_rename']
            for i in range(len(csv_path)):
                table_name_ = table_name[i]
                table_split_name_ = table_split_name[i]
                list_column_rename_ = list_column_rename[i]
                csv_path_ = csv_path[i]
                list_col_to_split_ = list_col_to_split[i]
                list_splitters_ = list_splitters[i]
                col_control_id_ = col_control_id[i]
                list_column_split_rename_ = list_column_split_rename[i]
                con = sqlite3.connect(self.db_path)
                print(list_column_rename_)
                if list_column_rename_ != '':
                    df = pd.read_csv(csv_path_, sep=',', names=list_column_rename_)
                else:
                    df = pd.read_csv(csv_path_, sep=',')
                self.insert_DataFrame_to_sqlite_table(
                    df,
                    table_name_,
                    csv_path_,
                    table_split_name_,
                    list_col_to_split_,
                    list_splitters_,
                    col_control_id_,
                    list_column_split_rename_
                )
                con.close()
        if _csv_path != '':
            con = sqlite3.connect(self.db_path)
            if _list_column_rename != '':
                df = pd.read_csv(_csv_path, sep=',', names=_list_column_rename)
            else:
                df = pd.read_csv(_csv_path, sep=',')
            self.insert_DataFrame_to_sqlite_table(
                df,
                _table_name,
                _csv_path,
                _table_split_name,
                _list_col_to_split,
                _list_splitters,
                _col_control_id,
                _list_column_split_rename
            )
            con.close()

    def insert_json_data_to_sqlite_table(self,
                                         yaml_file='',
                                         _json_path='',
                                         _list_column_rename='',
                                         _table_name='',
                                         _table_split_name='',
                                         _list_col_to_split='',
                                         _list_splitters='',
                                         _col_control_id='',
                                         _list_column_split_rename='',
                                         _lines=False
                                         ):
        """
        insert a json file in the database
        :param yaml_file:  the path of a yaml file containing the args below -> str
        :param _json_path: the path of the json file -> str
        :param _table_name: the name of the json once it's a table in the database
        :param _list_column_rename: if you want to modify the original column names of the json -> list
        :param _table_split_name: the name of the new table if their is a split (see field_split_method)
        :param _list_col_to_split: the list of column that will be split (see field_split_method)
        :param _list_splitters: the splitters used to split (see field_split_method)
        :param _col_control_id: the id_column to add to the split table (see field_split_method)
        :param _list_column_split_rename: how to rename the column once it was split (see field_split_method)
        :param _lines: Default False, use True if the json is written in lines != json style -> Boolean
        :return:
        """
        if yaml_file != '':
            yaml_file = open(yaml_file)
            yaml_content = yaml.load(yaml_file)
            yaml_dict = yaml_content['Pipeline_dict']
            json_path = yaml_dict['json_path']
            list_column_rename = yaml_dict['list_column_rename']
            list_col_to_split = yaml_dict['list_col_to_split']
            list_splitters = yaml_dict['list_splitters']
            col_control_id = yaml_dict['col_control_id']
            table_name = yaml_dict['table_name']
            table_split_name = yaml_dict['table_split_name']
            list_column_split_rename = yaml_dict['list_column_split_rename']
            lines = yaml_dict['lines']
            for i in range(len(table_name)):
                table_name_ = table_name[i]
                table_split_name_ = table_split_name[i]
                json_path_ = json_path[i]
                list_column_rename_ = list_column_rename[i]
                list_col_to_split_ = list_col_to_split[i]
                list_splitters_ = list_splitters[i]
                col_control_id_ = col_control_id[i]
                list_column_split_rename_ = list_column_split_rename[i]
                lines_ = lines[i]
                con = sqlite3.connect(self.db_path)
                if list_column_rename_ != '':
                    df = pd.read_json(json_path_, names=list_column_rename_, lines=lines_)
                else:
                    df = pd.read_json(json_path_, lines=lines_)
                self.insert_DataFrame_to_sqlite_table(
                    df,
                    table_name_,
                    json_path_,
                    table_split_name_,
                    list_col_to_split_,
                    list_splitters_,
                    col_control_id_,
                    list_column_split_rename_
                )
                con.close()
        if _json_path != '':
            con = sqlite3.connect(self.db_path)
            if _list_column_rename != '':
                df = pd.read_json(_json_path, names=_list_column_rename, lines=_lines)
            else:
                df = pd.read_json(_json_path, lines=_lines)
        self.insert_DataFrame_to_sqlite_table(
            df,
            _table_name,
            _json_path,
            _table_split_name,
            _list_col_to_split,
            _list_splitters,
            _col_control_id,
            _list_column_split_rename
        )
        con.close()

    def insert_files_from_folder_to_sqlite_tables(self, folder_path, sheet_name, table_name):
        """
        insert all the EXCEL files in a folder in the database as one single table, please note that the sheet names
        to insert has to be the same in EACH excel file
        :param folder_path: the path where the folder is located -> str
        :param sheet_name: the name of the sheet on which the table is located -> str
        :param table_name: the name of the final table in the database -> str
        """
        file_extensions = []
        headers_dict = {}
        df_dict_ = {}
        for file in os.listdir(folder_path):
            file_extensions.append(self.get_extension_from_file(file))
            df = pd.read_excel(os.path.join(folder_path,file), sheet_name)
            df.columns = [self._field_name_to_db_format(item) for item in list(df)]
            df_dict_[file] = df
            headers_dict[file] = [self._field_name_to_db_format(x) for x in list(df)]
        columns_ = headers_dict[self.get_max_len_header(headers_dict)]
        df = pd.DataFrame.from_dict(headers_dict, orient='index', columns=columns_)
        df = df.dropna(axis=1)
        df_to_concat = []
        for key in df_dict_:
            list_df_ = list(df.loc[key])
            list_df_dict = [self._field_name_to_db_format(x) for x in df_dict_[key]]
            if  len(list_df_dict) != len(list_df_):
                columns_to_drop = [i for i in list_df_dict + list_df_ if i not in list_df_dict or i not in list_df_]
                df_dict_[key] = df_dict_[key].drop(columns_to_drop, axis=1 )
            df_dict_[key].insert(0, 'Source', key)
            print(key)
            df_to_concat.append(df_dict_[key])
        final_df = pd.concat(df_to_concat)
        final_df['date'] = final_df['date'].astype('datetime64[ns]')
        self.insert_DataFrame_to_sqlite_table(final_df, table_name=table_name, source=folder_path)
        return final_df

    def fetch_dataframe_using_query(self, string='', file_path='', table_name=''):
        con = sqlite3.connect(self.db_path)
        if string != '':
            return pd.read_sql_query(string, con)
        if file_path != '':
            with open(file_path, 'r') as f:
                lines = f.read()
                return pd.read_sql_query(lines, con)
        if table_name != '':
            return pd.read_sql_table(table_name, con)

