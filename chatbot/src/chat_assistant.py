import os
import json 


from dotenv import load_dotenv
from typing import Dict, Any
from src.query_generator import MongoQueryGenerator

from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain


class ProcurementAssistant:
    def __init__(self, db):
        load_dotenv()
        self.db = db
        
        self.llm = OpenAI(
            model_name="gpt-4o",  
            temperature=0.7,
            openai_api_key=os.getenv('OPENAI_API_KEY')
        )
        
        self.system_context = """
        You are a procurement assistant who helps users understand and analyze California state procurement data.
        You can both answer general questions about procurement and analyze specific data from the database.
        
        The data includes:
        - Purchase orders from California state departments
        - Supplier information including small business and veteran-owned business status
        - Different types of acquisitions (IT Goods, IT Services, Non-IT Goods, Non-IT Services)
        - Financial details like unit price and total price
        - Temporal information including fiscal years
        
        Key capabilities:
        1. Answer general questions about procurement processes
        2. Explain terminology and concepts
        3. Query and analyze procurement data
        4. Provide insights and recommendations
        """
        
        self.prompt_template = PromptTemplate(
            input_variables=["prompt"],
            template=f"""
            {self.system_context}
            
            Respond to this prompt:
            {{prompt}}
            """
        )

        self.chain = LLMChain(
            llm=self.llm,
            prompt=self.prompt_template
        )
        
        self.query_classifier_template = PromptTemplate(
            input_variables=["message"],
            template="""
            Given this user message, determine if it:
            1. Requires querying the procurement database
            2. Is a general question about procurement
            3. Is a conversation/chat message
            4. Needs clarification
            
            User message: {message}
            
            Respond ONLY with a JSON object in this exact format with no other text or no double quoes before or after the opening or closing brackets:
            {{"type": "query|general|chat|clarify", "requires_data": true|false}}

            expected output: {{"type": "query", "requires_data": true}}
            """
        )
        
        self.classifier_chain = LLMChain(
            llm=self.llm,
            prompt=self.query_classifier_template
        )

    
    async def process_message(self, message: str) -> Dict[str, Any]:
        try:
            message_type = await self._classify_message(message)

            if message_type["type"] == "query":
                return await self._handle_data_query(message)
            
            elif message_type["type"] == "general":
                return await self._handle_general_question(message)
            
            elif message_type["type"] == "chat":
                return await self._handle_chat(message)
            
            else:
                return await self._request_clarification(message)
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _classify_message(self, message: str) -> Dict[str, Any]:
        try:
            response = await self.classifier_chain.arun({
                "message": message
            })
            
            print("LLM Response:", response)  # Debug print
            
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                print("Failed to parse JSON response:", response)
                return {
                    "type": "chat",
                    "requires_data": False
                }
                
        except Exception as e:
            print(f"Classification error: {str(e)}")
            return {
                "type": "chat",
                "requires_data": False
            }
    
    async def _handle_data_query(self, message: str) -> Dict[str, Any]:
        try:
            query_generator = MongoQueryGenerator()
            pipeline = await query_generator.generate_query(message)
            results = query_generator.execute_query(self.db, pipeline)
            
            response_prompt = f"""
            Given this user question: "{message}"
            And these query results: {str(results)[:1500]}  # Limit result length for prompt
            
            Generate a natural language response that:
            1. Directly answers the question
            2. Highlights key findings
            3. Includes relevant statistics
            4. Provides business context
            5. Formats numbers and dates clearly
            6. Mentions any limitations in the data
            
            Keep the response concise but informative.
            """
            
            response_text = await self.chain.arun({ "prompt": response_prompt })

            return {
                "success": True,
                "response": response_text,
                "data": results[:10],  # Limit results for API response
                "type": "data_query",
                "query": pipeline  # debugging
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "type": "data_query"
            }
    
    async def _handle_general_question(self, message: str) -> Dict[str, Any]:
        prompt = f"""
        Using your knowledge about procurement and the California state procurement system, answer this question:
        {message}
        
        Provide a clear, informative response that:
        1. Directly addresses the question
        2. Includes relevant context
        3. Uses procurement terminology appropriately
        4. Mentions if specific data would help answer the question better
        """
        
        response = await self.chain.arun({
            "prompt": prompt
        })

        
        return {
            "success": True,
            "response": response,
            "type": "general"
        }
    
    async def _handle_chat(self, message: str) -> Dict[str, Any]:
        try:
            prompt = f"""
            As a helpful procurement assistant, respond to this message:
            {message}
            
            Maintain a professional but friendly tone. If the conversation could benefit from focusing on procurement topics,
            gently guide it in that direction.
            """
            
            # Use chain.run() instead of llm.agenerate()
            response = await self.chain.arun({
                "prompt": prompt
            })
            
            return {
                "success": True,
                "response": response,
                "type": "chat"
            }
        except Exception as e:
            print(f"Error in chat handler: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "type": "chat"
            }
        
    async def _request_clarification(self, message: str) -> Dict[str, Any]:
        prompt = f"""
        Create a response that:
        1. Acknowledges the user's message: {message}
        2. Explains what's unclear
        3. Asks specific questions to clarify their needs
        4. Suggests possible interpretations
        """
        
        response = await self.chain.arun({
                "prompt": prompt
            })
        
        return {
            "success": True,
            "response": response,
            "type": "clarification"
        }