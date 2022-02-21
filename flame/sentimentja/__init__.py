import os
import sys
import sentimentja
from sentimentja.src import sentiment
from os.path import dirname, join
import youtokentome as yttm
from typing import Dict, List, Union


INPUT_TYPE = List[str]
OUTPUT_TYPE = List[Dict[str, Union[str, Dict[str, float]]]]


class Analyzer:
    def __init__(self):
        path = dirname(sentiment.__file__)
        self._maxlen = 310
        self._model = sentiment.load(join(path, "model_2022-01-10-14_55.h5"))
        self._bpe = yttm.BPE(model=join(path, "yttm_2022-01-10.model"))
        self._emolabels = [
            "happy", "sad", "angry", "disgust", "surprise", "fear"
        ]
        self._sentiment = sentiment

    def __call__(self, sentences: INPUT_TYPE) -> OUTPUT_TYPE:
        return self.analyze(sentences)

    def analyze(self, sentences: INPUT_TYPE) -> OUTPUT_TYPE:
        return list(sentiment.wrap(
            sentences,
            self._sentiment.predict(
                sentences,
                self._bpe.encode,
                self._model,
                self._maxlen).astype(float).tolist(),
            self._emolabels))        
