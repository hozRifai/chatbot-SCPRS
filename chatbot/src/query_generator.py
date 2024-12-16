import os
import json

from dotenv import load_dotenv
from langchain.llms import OpenAI
from langchain.chains import LLMChain
from typing import List, Dict, Any
from langchain.prompts import PromptTemplate

class MongoQueryGenerator:
    def __init__(self):
        load_dotenv()
        
        self.llm = OpenAI(
            model_name="gpt-4o",  
            temperature=0,  # Lower temperature for more precise outputs
            openai_api_key=os.getenv('OPENAI_API_KEY')
        )

        # Code to get random examples for each feature
        # for column_name in df.columns:
        #     v = df[column_name].dropna().unique()
        #     print( "Feature: ", column_name, " Value: ", v[0], " Type: ", df[column_name].dtype)
        
        # Define the schema information
        self.schema_info = """
        Database Schema for California Procurement Data:

        Collection: procurement_data
        Fields:
        - creation_date (string): System date when purchase order is entered.
        Example: "08/27/2013"
        - purchase_date (string): Date of purchase order is entered by the user.  This date can be back dated; therefore the creation date is primarily used.
        Example: "08/15/2013"
        - fiscal_year (string): Fiscal year derived based on creation date. State of California fiscal year starts on July 1 and ends on June 30.
        Example: "2013-2014"
        - lpa_number (string): Leveraged Procurement Agreement (LPA) Number, aka Contract Number.  If there is a contract number in this field, the amount is considered contract spend.
        Example: "7-12-70-26"
        - purchase_order_number (string): Purchase Order Number, numbers are not unique, different departments can have same purchase order number.
        Example: "REQ0011118"
        - requisition_number (string): Requisition Number, numbers are not unique, different departments can have same purchase order number.
        Example: "REQ0011118"
        - acquisition_type (string): Type of Acquisition: Non-IT Goods, Non-IT Services, IT Goods, IT Services
        Example: "IT Goods"
        - sub_acquisition_type (string): A sub-acquisition type depends on the acquisition type used.
        Example: "Personal Services"
        - acquisition_method (string): Type of acquisition used to make purchase.
        Example: "WSCA/Coop"
        - sub_acquisition_method (string): A sub-acquisition method depends on the acquisition method used.
        Example: "Other"
        - department_name (string): Name of purchasing department.
        Example: "Consumer Affairs, Department of"
        - supplier_code (float): Supplier Code
        Example: 1740272.0
        - supplier_name (string): Supplier name
        Example: "Pitney Bowes"
        - supplier_qualifications (string): Identifies supplier qualifications as a certified small business (SB), small business enterprise (SBE), disabled veteran business enterprise (DVBE), non-profits (NP), and micro business (MB). These qualifications are not mutually exclusive a supplier can be any combination of these. 
        Example: "CA-MB CA-SB"
        - supplier_zip_code (number): Suplier Zip Code.
        Example: 95811
        - calcard (string): State credit card (CalCard) used for purchased? YES/NO.
        Example: "NO"
        - item_name (string): Name of purchased items.
        Example: "Laptops"
        - item_description (string): Description of purchased items.
        Example: "15-inch laptops with 16GB RAM"
        - quantity (number): Quantity of items
        Example: 100
        - unit_price (number): Price per unit.
        Example: 1500.00
        - total_price (number): Total price excluding taxes/shipping.
        Example: 150000.00
        - classification_codes (number): United Nations Standard Products and Services Code® (UNSPSC) v. 14 of items purchased. This field may have more than one UNSPSC number based on the line items in the purchase order entered into eSCPRS.
        Example: 14111507
        - normalized_unspsc (float): Normalized UNSPSC code
        Example: 76121504.0
        - commodity_title (string): Correlated commodity tile based on the 8 digit Normalized UNSPSC.
        Example: "Jalapeno peppers"
        - class (float): Correlated class number based on the 8 digit normalized UNSPSC.
        Example: 50405600.0
        - class_title (string): Correlated class title based on the 8 digit Normalized UNSPSC 
        Example: "Peppers"
        - family (float): Correlated family number based on the 8 digit Normalized UNSPSC
        Example: 50400000.0
        - family_title (string):  Correlated family title based on the 8 digit Normalized UNSPSC
        Example: "Fresh vegetables"
        - segment (float):  Correlated segment number based on the 8 digit Normalized UNSPSC
        Example: 50000000.0
        - segment_title (string):  Correlated segment title based on the 8 digit Normalized UNSPSC
        Example: "Food Beverage and Tobacco Products"
        - location (string): location of the purchase 
        Example: "(38.662263, -121.346136)"

        
        Make sure to follow up with the format given in the examples here when you want to query the database.

        Important Notes:
        - The date fields (creation_date, purchase_date) are stored as strings in the format "MM/DD/YYYY".
        - When comparing dates, use the $dateFromString operator to convert the date strings to Date objects.
        Example: {{$dateFromString: {{dateString: "01/01/2012", format: "%m/%d/%Y"}}}}
        - Use the $gte and $lte operators for date range comparisons.
        - Aggregate and group data as needed to answer the question accurately.
        - Project only the necessary fields in the final output.


        """
        
        # Define the MongoDB query template
        self.query_template = """
        You are a MongoDB query generator for a procurement data analysis system.
        Using the provided schema, generate a MongoDB aggregation pipeline query to answer the user's question.
        
        Schema Information:
        {schema}
        
        Consider:
        1. Use proper MongoDB operators ($match, $group, $sort, etc.)
        2. Handle date fields properly (use $dateToString when displaying dates)
        3. Format numbers appropriately (use $sum, $avg as needed)
        4. Include proper sorting based on the question
        5. Limit results if appropriate
        
        User Question: {question}
        
        The output should be a valid MongoDB aggregation pipeline in this format:
        {{"pipeline": [{{"$stage": {{"field": "value"}}}}]}}

        do not include ``` or ```, only return a json object directly
        """
        
        self.prompt = PromptTemplate(
            input_variables=["schema", "question"],
            template=self.query_template
        )
        
        self.chain = LLMChain(
            llm=self.llm,
            prompt=self.prompt
        )
    
    async def generate_query(self, question: str) -> List[Dict[str, Any]]:        
        try:
            response = await self.chain.arun({
                "schema": self.schema_info,
                "question": question
            })

            print("Response for generate query: ", response)
                
            query_dict = json.loads(response)
            return query_dict["pipeline"]
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse generated query: {str(e)}")
        except KeyError:
            raise ValueError("Generated query missing 'pipeline' key")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def execute_query(self, db, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        try:
            results = list(db.procurement_data.aggregate(pipeline))
            return results
        except Exception as e:
            raise Exception(f"Error executing MongoDB query: {str(e)}")