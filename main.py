import mysql.connector as mysql
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import matplotlib.pyplot as plt
import seaborn as sns



class Crpm():
    def __init__(self):
        try:
            self.conn = mysql.connect(
                host="localhost",
                user="root",
                password="Groot",
                database="crm_db")
            self.cursor = self.conn.cursor()
            self.conn.commit()
            print('Connection succeed')

        except mysql.Error as e:
            print(f'Error creating connection {e}')
            self.conn = None
            self.cursor = None


    def create_customer_tables(self):
        try:
            cus_tab = '''CREATE TABLE IF NOT EXISTS customer(
                            customer_id INT AUTO_INCREMENT PRIMARY KEY,
                            name VARCHAR(50) NOT NULL,
                            gender VARCHAR(10) NOT NULL,
                            email VARCHAR(50) UNIQUE NOT NULL,
                            locality VARCHAR(100),
                            status ENUM('active', 'de-active') DEFAULT 'active',
                            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)'''
            
            self.cursor.execute(cus_tab)
            self.conn.commit()
            print('Customer table created successfully!')

        except Exception as e:
            print(f'Error creating customer table: {e}')    


    def create_product_table(self):
        try:
            prod_tab = '''CREATE TABLE IF NOT EXISTS product(
                            product_id INT AUTO_INCREMENT PRIMARY KEY,
                            name VARCHAR(50) NOT NULL,
                            price DECIMAL(10,2) NOT NULL,
                            stock INT DEFAULT 0,
                            status ENUM('active', 'de-active') DEFAULT 'active',
                            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)'''
            
            self.cursor.execute(prod_tab)
            self.conn.commit()
            print("Product table created successfully!")

        except Exception as e:
            print(f'Error creating Product table: {e}')


    def create_purchase_table(self):
        drop_tab = '''DROP TABLE IF EXISTS purchase'''
        self.cursor.execute(drop_tab)
        self.conn.commit()

        try:
            pur_tab = '''CREATE TABLE IF NOT EXISTS purchase(
                            purchase_id INT AUTO_INCREMENT PRIMARY KEY,
                            customer_id INT,
                            product_id INT,
                            quantity INT NOT NULL,
                            total_price DECIMAL(10,2) NOT NULL,
                            purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (customer_id) REFERENCES customer(customer_id) ON DELETE CASCADE,
                            FOREIGN KEY (product_id) REFERENCES product(product_id) ON DELETE CASCADE)'''
            
            self.cursor.execute(pur_tab)
            self.conn.commit()
            print("Purchase table created successfull!")
            st.success("Purchase Table Created Successfully!")

        except Exception as e:
            print(f'Error creating Purchase table: {e}')
            st.error(f'Error creating Purchase table: {e}')


    def create_tables(self):
        self.create_customer_tables()
        self.create_product_table()
        self.create_purchase_table()           


    def insert_customer_table(self, na, ge, em, loc):
        try:
            ins_cus = '''INSERT INTO customer (name, gender, email, locality) VALUES 
                                                    (%s, %s, %s, %s)'''
            self.cursor.execute(ins_cus, (na, ge, em, loc))
            self.conn.commit()
            print('Data Entered successfully on Customer table!')
        except Exception as e:
            print(f'Failed to update data on Customer table : {e}')


    def show_customer(self, a):
        #Active customers
        if a == 'active':
            cus = '''SELECT * FROM customer WHERE status = 'active';'''
            self.cursor.execute(cus)
            tab = self.cursor.fetchall()
            col_name = [des[0] for des in self.cursor.description]
            cust_tab = pd.DataFrame(tab, columns=col_name)

            return st.dataframe(cust_tab)

        #All customers
        elif a == 'all':
            cus1 = '''SELECT * FROM customer'''
            self.cursor.execute(cus1)
            tab1 = self.cursor.fetchall()
            col_name1 = [des[0] for des in self.cursor.description]
            cust_tab_all = pd.DataFrame(tab1, columns=col_name1)

            return st.dataframe(cust_tab_all)


    def updata_customer(self, id, name, a):
        if a == 'name':
            try:
                query = '''UPDATE customer SET name = %s WHERE customer_id = %s'''
                self.cursor.execute(query, (name, id))
                self.conn.commit()
                st.success(f'Update for {name} has been implemented successfully')

            except Exception as e:
                st.error(f'Error updating customer : {e}')
                self.conn.rollback() #Rollback in case of failure

        elif a == 'gender':
            try:
                query = '''UPDATE customer SET gender = %s WHERE customer_id = %s'''
                self.cursor.execute(query, (name, id))
                self.conn.commit()
                st.success(f'Update for gender {name} has been implemented successfully')

            except Exception as e:
                st.error(f'Error updating customer : {e}')
                self.conn.rollback() #Rollback in case of failure      

        elif a == 'email':
            try:
                query = '''UPDATE customer SET email = %s WHERE customer_id = %s'''
                self.cursor.execute(query, (name, id))
                self.conn.commit()
                st.success(f'Update for email has been implemented successfully')

            except Exception as e:
                st.error(f'Error updating customer : {e}')
                self.conn.rollback() #Rollback in case of failure        

        elif a == 'locality':
            try:
                query = '''UPDATE customer SET locality = %s WHERE customer_id = %s'''
                self.cursor.execute(query, (name, id))
                self.conn.commit()
                st.success(f'Update for locality has been implemented successfully')

            except Exception as e:
                st.error(f'Error updating customer : {e}')
                self.conn.rollback() #Rollback in case of failure             


    def deactivate_activate_delete_customer(self, cus_id, a): 
        #Deactivate
        if a == 'deactivate':
            try:   
                query_d = '''UPDATE customer SET status = 'de-active' WHERE customer_id = %s''' 
                self.cursor.execute(query_d, (cus_id,)) 
                self.conn.commit()
                st.success(f'Deactivated the customer {cus_id} successfully')

            except Exception as e:
                st.error(f'Error deactivating customer : {e}')
                self.conn.rollback() 

        #Activate
        elif a == 'activate':
            try:   
                query_a = '''UPDATE customer SET status = 'active' WHERE customer_id = %s''' 
                self.cursor.execute(query_a, (cus_id,)) 
                self.conn.commit()
                st.success(f'Activated the customer {cus_id} successfully')

            except Exception as e:
                st.error(f'Error Activating customer : {e}')
                self.conn.rollback()

        #Delete
        elif a == 'delete':
            try:   
                query_del = '''DELETE FROM customer WHERE customer_id = %s''' 
                self.cursor.execute(query_del, (cus_id,)) 
                self.conn.commit()
                st.success(f'Deleted the customer {cus_id} successfully')

            except Exception as e:
                st.error(f'Error Deleting customer : {e}')
                self.conn.rollback()   
    

    def insert_product_table(self, na, pr, st):
        try:
            ins_cus = '''INSERT INTO product (name, price, stock) VALUES 
                                                    (%s, %s, %s)'''
            self.cursor.execute(ins_cus, (na, pr, st))
            self.conn.commit()
            print('Data Entered successfully on Product table!')

        except Exception as e:
            print(f'Failed to update data on Product table : {e}')


    def show_product(self):
        cus = '''SELECT * FROM product'''
        self.cursor.execute(cus)
        tab = self.cursor.fetchall()
        col_name = [des[0] for des in self.cursor.description]
        cust_tab = pd.DataFrame(tab, columns=col_name)
        st.dataframe(cust_tab)


    def insert_purchase_table(self, ci, pi, qu):
        try:
            #Fetch the product price from the product table
            fetch_price = '''SELECT price FROM product WHERE product_id = %s AND status = 'active' LIMIT 1;'''
            self.cursor.execute(fetch_price, (pi,))
            product_price = self.cursor.fetchone()

            if product_price:
                tp = product_price[0] * qu

                ins_cus = '''INSERT INTO purchase (customer_id, product_id, quantity, total_price) VALUES 
                                                        (%s, %s, %s, %s)'''
                self.cursor.execute(ins_cus, (ci, pi, qu, tp))

                #Update the product stock
                update_query = '''UPDATE product SET stock = stock - %s
                WHERE product_id = %s'''
                self.cursor.execute(update_query, (qu, pi))

                self.conn.commit()
                st.success('Purchase details has been added')

        except Exception as e:
            st.success(f'Failed to update data on Purchase table : {e}')


    def show_purchase(self):
        cus = '''SELECT * FROM purchase'''
        self.cursor.execute(cus)
        tab = self.cursor.fetchall()
        col_name = [des[0] for des in self.cursor.description]
        cust_tab = pd.DataFrame(tab, columns=col_name)
        st.dataframe(cust_tab)


    def customer_analysis(self):
        cus = '''SELECT * FROM customer''' 
        self.cursor.execute(cus)
        tab1 = self.cursor.fetchall()
        col_name1 = [des[0] for des in self.cursor.description]
        cus_tab = pd.DataFrame(tab1, columns=col_name1)
        #st.dataframe(cus_tab)

        pur = '''SELECT * FROM purchase'''
        self.cursor.execute(pur)
        tab2 = self.cursor.fetchall()
        col_name2 = [des[0] for des in self.cursor.description]
        pur_tab = pd.DataFrame(tab2, columns=col_name2)
        #st.dataframe(pur_tab)

        #Joining customer and purchase table
        cus_pur = '''SELECT c.customer_id, c.name, pu.total_price FROM 
                                customer AS c INNER JOIN purchase AS pu
                                ON c.customer_id = pu.customer_id'''

        self.cursor.execute(cus_pur)
        tab3 = self.cursor.fetchall()
        col_name3 = [des[0] for des in self.cursor.description]
        cus_pur_tab = pd.DataFrame(tab3, columns=col_name3)
        #st.dataframe(cus_pur_tab)

        #Aggrecation
        cus_sales = cus_pur_tab.groupby('name')['total_price'].sum().reset_index()
        cur_sales = cus_sales.sort_values(by='total_price', ascending=False)

        #Plotting total sales per customer
        plt.figure(figsize=(10, 5))
        sns.barplot(data=cur_sales, x='name', y='total_price')
        plt.xlabel('Customer Name')
        plt.ylabel('Total Expense')
        plt.title('Total expense per Customer')
        st.pyplot(plt)

    def product_analysis(self):
        prod = '''SELECT * FROM product'''
        self.cursor.execute(prod)
        tab1 = self.cursor.fetchall()
        col_name1 = [des[0] for des in self.cursor.description]
        prod_tab = pd.DataFrame(tab1, columns=col_name1)
        #st.dataframe(prod_tab)

        pur = '''SELECT * FROM purchase'''
        self.cursor.execute(pur)
        tab2 = self.cursor.fetchall()
        col_name2 = [des[0] for des in self.cursor.description]
        pur_tab = pd.DataFrame(tab2, columns=col_name2)
        #st.dataframe(pur_tab)

        #Joining product and purchase table
        pro_pur = '''SELECT p.name, p.stock, pu.total_price FROM product AS p INNER JOIN
                        purchase AS pu ON
                        p.product_id = pu.product_id'''
        self.cursor.execute(pro_pur)
        tab3 = self.cursor.fetchall()
        col_name3 = [des[0] for des in self.cursor.description]
        pro_pur_tab = pd.DataFrame(tab3, columns=col_name3)
        #st.dataframe(pro_pur_tab)

        #Aggrecation
        prod_sales = pro_pur_tab.groupby('name')['total_price'].sum().reset_index()
        prod_sales = prod_sales.sort_values(by='total_price', ascending=False)
        #st.dataframe(prod_sales)


        #Plotting product revenue
        plt.figure(figsize=(10, 5))
        sns.barplot(data=prod_sales, x='name', y='total_price')
        plt.title('Product Revenues')
        plt.xlabel('Product Name')
        plt.ylabel('Total Sale')
        st.pyplot(plt)





#-----------------------------------------------MAIN PROGRAM-----------------------------------------------#


if __name__ == "__main__":
    st.title('Product Management System')
    st.subheader('Version -2.0')
    st.sidebar.title('CRM')

    tables = Crpm() 

    col1, col2 = st.columns(2)
    with col1:
        st.sidebar.markdown('Setup your warehouse')
        button = st.sidebar.button('SET-UP')

    if button:
        tables.create_tables()
        st.success('Database and tables have been successfully created!')


    option = option_menu('', ['Customer Management', 'Product Management', 'Purchase Management', 'Analytics'], orientation = 'horizontal', icons = ['check', 'check', 'check'])


    if option == 'Customer Management':
        radio = st.radio('Mode', ['VIEW', 'ADD', 'UPDATE'], index=0, horizontal=True)

        if radio == 'VIEW':
            st.markdown(':blue[**Active Customers**]')
            tables.show_customer('active')

        elif radio == 'ADD':
            col1, col2, col3 = st.columns(3)
            with col1:
                name = st.text_input("Name")
                sex = st.selectbox("Gender", ["Male", "Female", "Others"])

            with col3:    
                email = st.text_input("Email")
                loc = st.text_input("Locality")

            if st.button("Add to table"):
                tables.insert_customer_table(name, sex, email, loc)
                st.success(f'Customer {name} added to the table')

        elif radio == 'UPDATE':
            st.markdown(':blue[**Available customer details in the database**]')
            tables.show_customer('all')

            st.write('Choose the right tab to update the customer field')
            tab1, tab2, tab3, tab4 = st.tabs(['Name', 'Gender', 'Email', 'Locality'])

            with tab1:
                st.write(':blue[**Updating the existing customer**]')
                col1, col2 = st.columns(2)
                with col1:
                    cus_id = st.number_input("Customer ID", min_value=1, format='%d')

                with col2:
                    name = st.text_input("Name")

                enter = st.button('Enter')

                if enter and cus_id and name:
                    tables.updata_customer(cus_id, name, 'name') 

                st.markdown(':red[**Deactivate Customer**]')
                col1, col2, col3 = st.columns(3)
                with col1:
                    cus_id1 = st.number_input("Customer_ID", min_value=1, format='%d')

                with col3:
                    st.markdown('')
                    st.markdown('')
                    enter = st.button(':red[Deactivate]')
                    if enter and cus_id1:
                        tables.deactivate_activate_delete_customer(cus_id1, 'deactivate')


                st.markdown(':blue[**Activate Customer**]')
                col1, col2, col3 = st.columns(3)
                with col1:
                    cus_id2 = st.number_input("Customer_ID ", min_value=1, format='%d')

                with col3:
                    st.markdown('')
                    st.markdown('')
                    enter = st.button(':green[Activate]')
                    if enter and cus_id2:
                        tables.deactivate_activate_delete_customer(cus_id2, 'activate')   

                st.markdown(':red[**Delete Customer**]')
                col1, col2, col3 = st.columns(3)
                with col1:
                    cus_id3 = st.number_input("Customer_ID  ", min_value=1, format='%d')

                with col3:
                    st.markdown('')
                    st.markdown('')
                    enter = st.button(':red[Delete]')
                    if enter and cus_id3:
                        tables.deactivate_activate_delete_customer(cus_id3, 'delete')

            with tab2:
                #Gender
                st.write(':blue[**Updating the existing customer**]')
                col1, col2 = st.columns(2)
                with col1:
                    cus_id4 = st.number_input(" Customer_ID", min_value=1, format='%d')

                with col2:
                    gender = st.selectbox("Sex", ["Male", "Female", "Others"])

                enter1 = st.button('Enter ')

                if enter1 and cus_id4 and gender:
                    tables.updata_customer(cus_id4, gender, 'gender')

            with tab3:
                #Email
                st.write(':blue[**Updating the existing customer**]')
                col1, col2 = st.columns(2)
                with col1:
                    cus_id5 = st.number_input(" Customer_ID ", min_value=1, format='%d')

                with col2:
                    email = st.text_input("Email")

                enter2 = st.button(' Enter ')

                if enter2 and cus_id5 and email:
                    tables.updata_customer(cus_id5, email, 'email')

            with tab4:
                #Locality
                st.write(':blue[**Updating the existing customer**]')
                col1, col2 = st.columns(2)
                with col1:
                    cus_id6 = st.number_input(" Customer ID ", min_value=1, format='%d')

                with col2:
                    locality = st.text_input("Locality")

                enter3 = st.button(' Enter  ')

                if enter3 and cus_id6 and locality:
                    tables.updata_customer(cus_id6, locality, 'locality')        


    elif option == 'Product Management':
        radio = st.radio('Mode', ['VIEW', 'ADD', 'UPDATE'], index=0, horizontal=True)

        if radio == 'VIEW':
            tables.show_product()

        elif radio == 'ADD':
            col1, col2, col3 = st.columns(3)
            with col1:
                name = st.text_input("Name")

            with col2:
                price = st.number_input("Price") 

            with col3:    
                stock = st.number_input("Stock", min_value=1, format="%d")

            if st.button("Add to table"):
                tables.insert_product_table(name, price, stock)
                st.success(f'Product {name} added to the table')

        elif radio == 'UPDATE':
            tables.show_product()

            st.write('Choose the right tab to update the product field')
            tab1, tab2, tab3, tab4, tab5 = st.tabs(['Name', 'Price', 'Stock', 'Status', 'Deactivate-product'])

            with tab1:
                st.write('Updating the existing customer')
                col1, col2 = st.columns(2)
                with col1:
                    cus_id = st.number_input("Customer ID", min_value=1, format='%d')

                with col2:
                    name = st.text_input("Name")

                enter = st.button('Enter')
                if enter:
                    tables.updata_customer(cus_id, name)        



    elif option == 'Purchase Management':
        radio = st.radio('Mode', ['VIEW', 'ADD'], index=0, horizontal=True)

        if radio == 'VIEW':
            tables.show_purchase()

        elif radio == 'ADD':
            col1, col2, col3 = st.columns(3)
            with col1: 
                cus_id = st.number_input("Customer ID", min_value=1, format="%d")
                prod_id = st.number_input("Product ID", min_value=1, format="%d")

            with col3:    
                qty = st.selectbox("Quantity", [i for i in range(1, 31)])

            if st.button("Add to table"):
                tables.insert_purchase_table(cus_id, prod_id, qty)


    elif option == 'Analytics':
        tables.customer_analysis()

        st.markdown('')
        st.markdown('')

        tables.product_analysis()   



              