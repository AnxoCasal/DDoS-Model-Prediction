import torch.nn as nn

class ResidualBlock(nn.Module):
    def __init__(self, in_features, out_features):
        super(ResidualBlock, self).__init__()
        self.linear1 = nn.Linear(in_features, out_features)
        self.relu = nn.ReLU(inplace=True)
        self.linear2 = nn.Linear(out_features, out_features)
        
        if in_features != out_features:
            self.shortcut = nn.Sequential(
                nn.Linear(in_features, out_features),
                nn.BatchNorm1d(out_features),
                nn.ReLU(inplace=True)
            )
        else:
            self.shortcut = nn.Identity()

    def forward(self, x):
        residual = self.shortcut(x)
        out = self.linear1(x)
        out = self.relu(out)
        out = self.linear2(out)

        out += residual

        out = self.relu(out)
        return out
    
class LastHope(nn.Module):
    '''
        LastHope representa la última defensa, el baluarte final en la lucha por un modelo competente.
        Con el poder de los bloques residuales, esta red es nuestra Odisea moderna, destinada a superar
        los obstáculos del desvanecimiento del gradiente y la ineficiencia en el aprendizaje.
    
        En un mar de incertidumbre, LastHope es la luz de Pharos, la promesa de precisión y robustez.
        Si fallamos aquí, todo estará perdido.
    
        Args:
            input_features (int): Características de entrada.
            num_classes (int): Clases de salida.
            hidden_features (list): Dimensiones de capas ocultas.
    '''
    def __init__(self, input_features=4, num_classes=12, hidden_features=[64, 128, 256, 128, 64]):
        super(LastHope, self).__init__()
        self.input_layer = nn.Linear(input_features, hidden_features[0])
        
        layers = []
        in_features = hidden_features[0]
        for out_features in hidden_features:
            layers.append(ResidualBlock(in_features, out_features))
            in_features = out_features
        
        self.residual_blocks = nn.Sequential(*layers)
        self.output_layer = nn.Linear(hidden_features[-1], num_classes)
    
    def forward(self, x):
        x = self.input_layer(x)
        x = self.residual_blocks(x)
        x = self.output_layer(x)
        return x