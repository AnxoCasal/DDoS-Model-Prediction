import torch.nn as nn

class ResidualBlock(nn.Module):
    def __init__(self, in_features, out_features):
        super(ResidualBlock, self).__init__()
        self.linear1 = nn.Linear(in_features, out_features)
        self.relu = nn.ReLU()
        self.linear2 = nn.Linear(out_features, out_features)
        
        if in_features != out_features:
            self.shortcut = nn.Sequential(
                nn.Linear(in_features, out_features, bias=False),
            )
        else:
            self.shortcut = nn.Identity()

    def forward(self, x):
        residual = self.shortcut(x)
        out = self.relu(self.linear1(x))
        out = self.linear2(out)

        out += residual

        out = self.relu(out)
        return out
    
class ResNet(nn.Module):
    '''
        Args:
            input_features (int): Características de entrada.
            num_classes (int): Clases de salida.
            hidden_features (list): Dimensiones de capas ocultas.
    '''
    def __init__(self, input_features=6, num_classes=7, hidden_features=[64, 128, 256, 128, 64]):
        super(ResNet, self).__init__()
        self.input_layer = nn.Linear(input_features, hidden_features[0])
        
        layers = []
        in_features = hidden_features[0]
        for out_features in hidden_features:
            layers.append(ResidualBlock(in_features, out_features))
            in_features = out_features
        
        self.residual_blocks = nn.Sequential(*layers)
        self.output_layer = nn.Linear(hidden_features[-1], num_classes)

        self.relu = nn.ReLU()
    
    def forward(self, x):
        x = self.relu(self.input_layer(x))
        x = self.residual_blocks(x)
        x = self.output_layer(x)
        return x
