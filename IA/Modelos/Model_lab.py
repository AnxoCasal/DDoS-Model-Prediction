import torch
import pandas as pd
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import StratifiedKFold
import torch.optim as optim
import numpy as np
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader, TensorDataset

class NN_01(nn.Module):
   def __init__(self):
      super(NN_01, self).__init__()
      self.fc1 = nn.Linear(4, 32)
      self.fc2 = nn.Linear(32, 64)
      self.fc3 = nn.Linear(64, 128)
      self.fc4 = nn.Linear(128, 256)
      self.fc5 = nn.Linear(256, 256)
      self.fc6 = nn.Linear(256, 128)
      self.fc7 = nn.Linear(128, 64)
      self.fc8 = nn.Linear(64, 12)

      self.flat = nn.Flatten()
      self.relu = nn.LeakyReLU(negative_slope=0.01)

   def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.relu(self.fc3(x))
        x = self.relu(self.fc4(x))
        x = self.relu(self.fc5(x))
        x = self.relu(self.fc6(x))
        x = self.relu(self.fc7(x))
        x = self.fc8(x)
        return x
   
class NN_02(nn.Module):
   def __init__(self):
      super(NN_02, self).__init__()
      self.fc1 = nn.Linear(4, 32)
      self.fc2 = nn.Linear(32, 64)
      self.fc3 = nn.Linear(64, 32)
      self.fc4 = nn.Linear(32, 12)

      self.flat = nn.Flatten()
      self.relu = nn.ELU()

   def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.relu(self.fc3(x))
        x = self.fc4(x)
        return x
   
class NN_03(nn.Module):
   def __init__(self):
      super(NN_03, self).__init__()
      self.fc1 = nn.Linear(4, 32)
      self.fc2 = nn.Linear(32, 64)
      self.fc3 = nn.Linear(64, 128)
      self.fc4 = nn.Linear(128, 256)
      self.fc5 = nn.Linear(256, 128)
      self.fc6 = nn.Linear(128, 64)
      self.fc7 = nn.Linear(64, 12)

      self.flat = nn.Flatten()
      self.relu = nn.ReLU()

   def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.relu(self.fc3(x))
        x = self.relu(self.fc4(x))
        x = self.relu(self.fc5(x))
        x = self.relu(self.fc6(x))
        x = self.fc7(x)
        return x
   
class NN_04(nn.Module):
   def __init__(self):
      super(NN_04, self).__init__()
      self.fc1 = nn.Linear(4, 32)
      self.fc2 = nn.Linear(32, 64)
      self.fc3 = nn.Linear(64, 128)
      self.fc4 = nn.Linear(128, 256)
      self.fc5 = nn.Linear(256, 512)
      self.fc6 = nn.Linear(512, 256)
      self.fc7 = nn.Linear(256, 128)
      self.fc8 = nn.Linear(128, 64)
      self.fc9 = nn.Linear(64, 12)

      self.flat = nn.Flatten()
      self.relu = nn.ELU()

   def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.relu(self.fc3(x))
        x = self.relu(self.fc4(x))
        x = self.relu(self.fc5(x))
        x = self.relu(self.fc6(x))
        x = self.relu(self.fc7(x))
        x = self.relu(self.fc8(x))
        x = self.fc9(x)
        return x