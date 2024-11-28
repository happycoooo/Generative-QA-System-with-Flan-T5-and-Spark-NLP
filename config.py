from ray import tune

data_dir = '/data/lab/project2/squad_v2/squad_v2/train-00000-of-00001.parquet'
model_dir='/data/lab/project2/flan-t5-small'
learning_rate = 1e-4  # 在tune中被证明这个learning rate是最好的
eval_steps = 2  
batch_size = 16
num_workers = 1
num_train_epochs=3
save_total_limit=2
gradient_accumulation_steps=6
test_size=0.02
search_space = {"lr": tune.choice([1e-3, 5e-4, 1e-4, 5e-5, 1e-5])}