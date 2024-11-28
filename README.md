# Generative-QA-System-with-Flan-T5-and-Spark-NLP
In this project, I designed and built a generative question-answering (QA) system using the SQuAD v2 dataset. The goal was to create a system capable of answering questions based on provided contexts by fine-tuning the Flan-T5-small model. This project was divided into four main tasks:

Data Processing: I processed the SQuAD v2 dataset by using the context and questions as inputs, with the answers as outputs. I utilized the official validation set as the test set and split the original training data into a training set and a validation set (5000 samples for the validation set, while the rest formed the training set). The data was then formatted according to the requirements for training models like T5 and Flan-T5. I accomplished this using Python, ensuring the data was ready for model training.

Model Training: I wrote both a bash script and a Python script to train the QA model, leveraging the Ray framework for distributed training. I used Flan-T5-small as the base model and performed further fine-tuning to adapt it for QA tasks. Due to the lack of GPU availability on the server, I used PyTorch with a CPU to debug the code and conduct initial training for a few hours. The model checkpoint was saved to continue the training later. The validation set was used for hyperparameter tuning and selecting the optimal model checkpoint. I also referred to training examples from the Hugging Face repository and considered renting a GPU server through AutoDL to continue training.

Model Deployment: I deployed the fine-tuned QA model using Spark-NLP, allowing it to process and answer questions in real-time via Kafka for streaming. I adapted the deployment to use a Spark-NLP model that wasn't the original Flan-T5-small, ensuring compatibility with the streaming infrastructure. This approach enabled efficient streaming processing, where the system could take questions as input and return generated answers continuously.

Survey Paper Review: I reviewed a survey paper on retrieval-augmented generation (RAG) for open-domain question answering. I illustrated a possible method to enhance open-domain QA with a system diagram and pipeline introduction. This involved explaining how a retrieval module could gather relevant context from a large corpus of documents, followed by a generative model generating the final answer using the retrieved context. The diagram and pipeline highlighted how RAG techniques could provide accurate answers by leveraging relevant information effectively.

Overall, this project aimed to create an effective and deployable question-answering system by using modern NLP tools and models, experimenting with distributed training approaches, and deploying the model for real-time usage through streaming frameworks.

The code and resources for this project are structured as follows:

Data Preparation: Scripts for processing and formatting the SQuAD v2 dataset.

Training: Bash and Python scripts for training the Flan-T5-small model using Ray, along with configuration files for hyperparameter tuning.

Deployment: Spark-NLP deployment scripts for real-time streaming using Kafka.

Documentation: A detailed explanation of the retrieval-augmented generation method for open-domain QA, including diagrams and pipeline descriptions.

Feel free to explore the codebase for more details on each component of the project.
