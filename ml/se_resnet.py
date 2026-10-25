"""
SE-ResNet18, the model the training project trains. Its layer names must
match the training project's model.py so saved weights load.
"""

import torch
import torch.nn as nn
from torchvision import models


class SEBlock(nn.Module):
    def __init__(self, channels: int, reduction: int = 16):
        super().__init__()
        reduced = max(channels // reduction, 1)
        self.squeeze = nn.AdaptiveAvgPool2d(1)
        self.excitation = nn.Sequential(
            nn.Linear(channels, reduced, bias=False),
            nn.ReLU(inplace=True),
            nn.Linear(reduced, channels, bias=False),
            nn.Sigmoid(),
        )

    def forward(self, x):
        b, c, _, _ = x.size()
        y = self.excitation(self.squeeze(x).view(b, c)).view(b, c, 1, 1)
        return x * y


class SEResNet18(nn.Module):
    def __init__(self, num_classes: int):
        super().__init__()
        base = models.resnet18(weights=None)
        self.conv1, self.bn1, self.relu, self.maxpool = base.conv1, base.bn1, base.relu, base.maxpool
        self.layer1 = self._inject_se(base.layer1)
        self.layer2 = self._inject_se(base.layer2)
        self.layer3 = self._inject_se(base.layer3)
        self.layer4 = self._inject_se(base.layer4)
        self.avgpool = base.avgpool
        self.fc = nn.Linear(base.fc.in_features, num_classes)

    @staticmethod
    def _inject_se(layer):
        for block in layer:
            block.se = SEBlock(block.conv2.out_channels)

            def forward_with_se(x, block=block):
                identity = x
                out = block.relu(block.bn1(block.conv1(x)))
                out = block.se(block.bn2(block.conv2(out)))
                if block.downsample is not None:
                    identity = block.downsample(x)
                return block.relu(out + identity)

            block.forward = forward_with_se
        return layer

    def forward(self, x):
        x = self.maxpool(self.relu(self.bn1(self.conv1(x))))
        x = self.layer4(self.layer3(self.layer2(self.layer1(x))))
        return self.fc(torch.flatten(self.avgpool(x), 1))
