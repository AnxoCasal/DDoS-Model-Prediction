import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import pandas as pd
from torch.utils.data import DataLoader, Subset, TensorDataset
from Modelos.LNN_01 import SimpleNN
from sklearn.model_selection import StratifiedKFold, StratifiedShuffleSplit
import wandb
from Modelos.Model_lab import *

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
DATASET_PATH = './Entrenamiento/training_set.parquet'

strat_splitter = StratifiedShuffleSplit(n_splits=1, test_size=0.15, random_state=2)

df = pd.read_parquet(DATASET_PATH)
X = df.drop('Label_Index', axis=1).values
y = df['Label_Index'].values

train_idx, test_idx = next(strat_splitter.split(X, y))
x_train = torch.Tensor(X[train_idx])
y_train = torch.Tensor(y[train_idx]).long()
x_test = torch.Tensor(X[test_idx])
y_test = torch.Tensor(y[test_idx]).long()
train_data = TensorDataset(x_train, y_train)
test_data = TensorDataset(x_test, y_test)

print(max(test_data.tensors[1]))
for n in test_data.tensors[1]:
    if n == 4:
        print('AAAAAAA')
        break

print([int(n) for n in test_data.tensors[1]])
