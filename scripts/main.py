from instacart_db import InstacartDb
import pandas as pd
import os

def main():
    data_dir = './data'
    if not os.path.exists(data_dir):
        data_dir = '../data'
    
    aisles = pd.read_csv(f"{data_dir}/aisles.csv", sep=';', low_memory=False, index_col=None)
    aisles.name = 'aisles'
    departments = pd.read_csv(f"{data_dir}/departments.csv", sep=';', low_memory=False, index_col=None)
    departments.name = 'departments'
    orders = pd.read_csv(f"{data_dir}/instacart_orders.csv", sep=';', low_memory=False, index_col=None)
    orders.name = 'orders'
    order_products = pd.read_csv(f"{data_dir}/order_products.csv", sep=';', low_memory=False, index_col=None)
    order_products.name = 'order_products'
    order_products['reordered'] = order_products['reordered'].astype(bool)
    products = pd.read_csv(f"{data_dir}/products.csv", sep=';', low_memory=False, index_col=None)
    products.name = 'products'

    dataframes = [aisles, orders, departments, products, order_products]

    with InstacartDb() as db:
        db.create_aisles_table()
        db.create_departments_table()
        db.create_products_table()
        db.create_orders_table()
        db.create_order_products_table()
        print('Tables created')

        for df in dataframes:
            tabla = df.name.upper()
            columnas = ', '.join(df.columns).upper()
            values = []
            sql = None
            result = None
            try:
                chunk_size = len(df) // 5
                for chunk in range(5):
                    start = chunk * chunk_size
                    end = None if chunk == 4 else (chunk + 1) * chunk_size
                    df_chunk = df.iloc[start:end]
                    
                    for i, row in df_chunk.iterrows():
                        datos = []
                        for col in df.columns:
                            if type(row[col]) == str:
                                datos.append(f'"{row[col]}"')   
                            elif pd.isnull(row[col]):
                                datos.append('NULL')
                            else:
                                datos.append(str(row[col]))
                        datos = '(' + ', '.join(datos) + ')'
                        values.append(datos)
                    values = ', \n'.join(values)
                    sql = f"""INSERT IGNORE INTO {tabla} ({columnas}) VALUES {values}"""
                    result = db.excecute_sql(sql)
                    values = []  
                print(f'Data inserted into {tabla} table')
            except Exception as e:
                print(f'Error inserting data into {tabla} table')
                print(result)
                print(sql)
                break

if __name__ == '__main__':
    main()