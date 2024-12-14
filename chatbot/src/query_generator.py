from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from langchain.chains import LLMChain
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import json
import os
from dotenv import load_dotenv

class MongoQueryGenerator:
    def __init__(self):
        load_dotenv()
        
        # Initialize OpenAI
        self.llm = OpenAI(
            model_name="gpt-4o",  
            temperature=0,  # Lower temperature for more precise outputs
            openai_api_key=os.getenv('OPENAI_API_KEY')
        )
        
        # Define the schema information
        self.schema_info = """
        Database Schema for California Procurement Data:

        Collection: procurement_data
        Fields:
        - creation_date (datetime): System date when purchase order is entered
        Example: "08/27/2013"
        - purchase_date (datetime): Date of purchase order entered by user
        Example: "08/15/2013"
        - fiscal_year (string): State of CA fiscal year (July 1-June 30)
        Example: "2013/2014"
        - lpa_number (string): Leveraged Procurement Agreement/Contract Number
        Example: "1-19-70-01A"
        - purchase_order_number (string): PO identifier
        Example: "P132000012345"
        - requisition_number (string): Internal request number
        Example: "REQ-1234"
        - acquisition_type (string): IT Goods, IT Services, Non-IT Goods, Non-IT Services
        Example: "IT Goods"
        - sub_acquisition_type (string): Sub-category of acquisition type
        Example: "Hardware"
        - acquisition_method (string): Procurement method
        Example: "Competitive Bid"
        - sub_acquisition_method (string): Sub-category of procurement method
        Example: "Invitation for Bid"
        - department_name (string): Name of purchasing department
        Example: "Department of Technology"
        - supplier_name (string): Supplier name
        Example: "ABC Inc."
        - supplier_qualifications (string): SB, SBE, DVBE, NP, MB certifications
        Example: "SB, DVBE"
        - item_name (string): Name of purchased items
        Example: "Laptops"
        - item_description (string): Description of purchased items
        Example: "15-inch laptops with 16GB RAM"
        - quantity (number): Quantity of items
        Example: 100
        - unit_price (number): Price per unit
        Example: 1500.00
        - total_price (number): Total price excluding taxes/shipping
        Example: 150000.00
        - normalized_unspsc (string): Normalized UNSPSC code
        Example: "43211503"
        - commodity_title (string): Title of the commodity
        Example: "Notebook Computers"
        - classification_codes (string): UNSPSC codes
        Example: "43211503"

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
        
        # Create the chain
        self.chain = LLMChain(
            llm=self.llm,
            prompt=self.prompt
        )
    
    async def generate_query(self, question: str) -> List[Dict[str, Any]]:
        """
        Generate MongoDB aggregation pipeline from natural language question
        
        Args:
            question (str): Natural language question about procurement data
            
        Returns:
            List[Dict]: MongoDB aggregation pipeline
        """

        print("In generate_query")
        # Generate the query
        
        
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
        """
        Execute generated MongoDB pipeline
        
        Args:
            db: MongoDB database instance
            pipeline (List[Dict]): Aggregation pipeline to execute
            
        Returns:
            List[Dict]: Query results
        """
        try:
            results = list(db.procurement_data.aggregate(pipeline))
            return results
        except Exception as e:
            raise Exception(f"Error executing MongoDB query: {str(e)}")

class QueryValidator:
    """
    Validate and sanitize MongoDB queries before execution
    """
    @staticmethod
    def validate_pipeline(pipeline: List[Dict[str, Any]]) -> bool:
        """
        Basic validation of MongoDB pipeline
        
        Args:
            pipeline (List[Dict]): Pipeline to validate
            
        Returns:
            bool: True if pipeline is valid
        """
        # List of allowed MongoDB operators
        allowed_operators = {
            "$match", "$group", "$sort", "$limit", "$project", 
            "$unwind", "$lookup", "$dateToString", "$sum", "$avg"
        }
        
        def check_dict(d: Dict) -> bool:
            for key, value in d.items():
                if key.startswith("$"):
                    if key not in allowed_operators:
                        return False
                if isinstance(value, dict):
                    if not check_dict(value):
                        return False
            return True
        
        try:
            for stage in pipeline:
                if not isinstance(stage, dict):
                    return False
                if not check_dict(stage):
                    return False
            return True
        except Exception:
            return False