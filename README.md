**Project Overview**

This project implements an AI-powered ETL (Extract, Transform, Load) pipeline designed to automate data validation and improve data quality using a combination of rule-based checks and LLM-based intelligent validation.

The pipeline processes structured data, applies validation rules, detects anomalies, and enhances data quality with minimal manual intervention.

**Key Features**

•	End-to-end ETL pipeline (Extract → Transform → Load)

•	Rule-based data validation (null checks, format checks, constraints)

•	AI-powered validation using OpenAI (Hybrid approach)

•	Automated data quality checks and anomaly detection

•	Logging and validation reporting

•	Scalable and modular design

**Architecture**

1.**	Extract**

•	Load raw data from source (CSV / database)

2.	**Transform**
•	Clean and preprocess data
•	Handle missing values and formatting issues

3.	**Validate**
•	Rule-based validation
•	AI-based validation using LLM

4.	**Load**
•	Store validated data into target system

**Tech Stack**

•	Python

•	Pandas / NumPy

•	SQL (Snowflake / relational DB)

•	OpenAI API (LLM validation)

•	Logging & Data Quality Frameworks

**Project Structure**

Hybrid-ai-data-validation-pipeline/

│

├── etl_pipeline.py

├── config.py

├── requirements.txt

├── README.md

**How to Run**

1. **Clone Repository**
git clone https://github.com/Aruna-Mathiy/Hybrid-ai-data-validation-pipeline.git

cd Hybrid-ai-data-validation-pipeline

2. **Install Dependencies**
pip install -r requirements.txt

3. **Set Environment Variables**
   
Create a .env file:

OPENAI_API_KEY=your_api_key_here

4. **Run Pipeline**
   
python etl_pipeline.py

**Sample Output**

•	Cleaned dataset

•	Validation summary

•	Error logs for invalid records

**Key Achievements**

•	Automated 90%+ of manual data validation tasks

•	Reduced data quality issues significantly

•	Combined rule-based + AI validation for better accuracy

**Future Enhancements**

•	Add real-time data streaming support

•	Build a dashboard using Power BI

•	Integrate with cloud platforms (AWS / Azure)

•	Enhance anomaly detection models







