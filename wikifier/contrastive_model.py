import glob
import torch
import torch.nn as nn
import torch.nn.functional as F
import pandas as pd
import numpy as np
from torch.utils.data import Dataset, DataLoader
from torch.optim import Adam
import os
import pickle
from itertools import chain

# Dataset
class T2DV2Dataset(Dataset):
    def __init__(self, pos_features, neg_features):
        self.pos_features = pos_features
        self.neg_features = neg_features
    
    def __len__(self):
        return len(self.pos_features)
    
    def __getitem__(self, idx):
        return self.pos_features[idx], self.neg_features[idx]

# Model
class PairwiseNetwork(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        #original 12x24, 24x12, 12x12, 12x1
        self.fc1 = nn.Linear(hidden_size, 2*hidden_size)
        self.fc2 = nn.Linear(2*hidden_size, hidden_size)
        self.fc3 = nn.Linear(hidden_size, hidden_size)
        self.fc4 = nn.Linear(hidden_size, 1)
    
    def forward(self, pos_features, neg_features):
        # Positive pass
        x = F.relu(self.fc1(pos_features))
        x = F.relu(self.fc2(x))
        x = F.relu(self.fc3(x))
        pos_out = torch.sigmoid(self.fc4(x))
        
        # Negative Pass
        x = F.relu(self.fc1(neg_features))
        x = F.relu(self.fc2(x))
        x = F.relu(self.fc3(x))
        neg_out = torch.sigmoid(self.fc4(x))
        
        return pos_out, neg_out
    
    def predict(self, test_feat):
        x = F.relu(self.fc1(test_feat))
        x = F.relu(self.fc2(x))
        x = F.relu(self.fc3(x))
        test_out = torch.sigmoid(self.fc4(x))
        return test_out

# Pairwise Loss
class PairwiseLoss(nn.Module):
    def __init__(self):
        super().__init__()
        self.m = 0
    
    def forward(self, pos_out, neg_out):
        distance = (1 - pos_out) + neg_out
        loss = torch.mean(torch.max(torch.tensor(0), distance))
        return loss