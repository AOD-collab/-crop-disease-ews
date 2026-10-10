"""
SE-ResNet18 — the architecture trained by the separate crop_disease_ews
training project (its model.py). Kept here so ml/inference.py can load a
checkpoint whose "architecture" is "se_resnet18" without importing anything
from the training project.

The state_dict keys must stay identical to the training project's model.py:
a torchvision ResNet-18 whose residual blocks each carry a squeeze-and-
excitation module at `block.se`, applied after bn2 and before the residual
add. The top-level attribute names (conv1, bn1, relu, maxpool, layer1-4,
avgpool, fc) are the same as torchvision's ResNet, so the hand-run Grad-CAM
in ml/inference.py's disease_heatmap() works on this model unchanged.
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
