/*
Copyright (C) 2014 Sergey Demyanov. 
contact: sergey@demyanov.net
http://www.demyanov.net

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "net.h"
#include "layer_i.h"
#include "layer_j.h"
#include "layer_n.h"
#include "layer_c.h"
#include "layer_s.h"
#include "layer_t.h"
#include "layer_f.h"
#include <ctime>

void Net::InitLayers(const mxArray *mx_layers) {
  
  //mexPrintMsg("Start layers initialization...");
  size_t layers_num = mexGetNumel(mx_layers);  
  mexAssert(layers_num >= 2, "The net must contain at least 2 layers");
  const mxArray *mx_layer = mexGetCell(mx_layers, 0);  
  std::string layer_type = mexGetString(mexGetField(mx_layer, "type"));   
  mexAssert(layer_type == "i", "The first layer must be the type of 'i'");
  layers_.resize(layers_num);
  layers_[0] = new LayerInput();
  //mexPrintMsg("Initializing layer of type", layer_type);    
  layers_[0]->Init(mx_layer, NULL); 
  for (size_t i = 1; i < layers_num; ++i) {    
    Layer *prev_layer = layers_[i-1];
    mx_layer = mexGetCell(mx_layers, i);  
    layer_type = mexGetString(mexGetField(mx_layer, "type"));
    if (layer_type == "j") {
      layers_[i] = new LayerJitt();
    } else if (layer_type == "n") {      
      layers_[i] = new LayerNorm();
    } else if (layer_type == "c") {      
      layers_[i] = new LayerConv();
    } else if (layer_type == "s") {
      layers_[i] = new LayerScal();
    } else if (layer_type == "t") {
      layers_[i] = new LayerTrim();
    } else if (layer_type == "f") {
      layers_[i] = new LayerFull();
    } else {
      mexAssert(false, layer_type + " - unknown type of the layer");
    }    
    //mexPrintMsg("Initializing layer of type", layer_type);    
    layers_[i]->Init(mx_layer, prev_layer);    
  }
  mexAssert(layer_type == "f", "The last layer must be the type of 'f'");
  mexAssert(layers_.back()->function_ == "soft" || 
            layers_.back()->function_ == "sigm" || 
            layers_.back()->function_ == "SVM" || 
            layers_.back()->function_ == "linear" ||
            layers_.back()->function_ == "relu",
            "The last layer function must be 'soft', 'sigm' or 'SVM'");
  //mexPrintMsg("Layers initialization finished");
}

void Net::InitParams(const mxArray *mx_params) {
  //mexPrintMsg("Start params initialization...");
  params_.Init(mx_params);
  //mexPrintMsg("Params initialization finished");
}

void Net::Train(const mxArray *mx_data, const mxArray *mx_labels) {  
  
  //mexPrintMsg("Start training...");  
  ReadData(mx_data);
  ReadLabels(mx_labels);
  InitNorm();
  
  std::srand(params_.seed_);  
  
  size_t train_num = labels_.size1();
  size_t numbatches = (size_t) ceil((ftype) train_num/params_.batchsize_);
  trainerror_.resize(params_.numepochs_, numbatches);
  for (size_t epoch = 0; epoch < params_.numepochs_; ++epoch) {    
    std::vector<size_t> randind(train_num);
    for (size_t i = 0; i < train_num; ++i) {
      randind[i] = i;
    }
    if (params_.shuffle_) {
      std::random_shuffle(randind.begin(), randind.end());
    }
    std::vector<size_t>::const_iterator iter = randind.begin();
    for (size_t batch = 0; batch < numbatches; ++batch) {
      size_t batchsize = std::min(params_.batchsize_, (size_t)(randind.end() - iter));
      std::vector<size_t> batch_ind = std::vector<size_t>(iter, iter + batchsize);
      iter = iter + batchsize;      
      Mat data_batch = SubMat(data_, batch_ind, 1);      
      Mat labels_batch = SubMat(labels_, batch_ind, 1);      
      UpdateWeights(epoch, false);      
      InitActiv(data_batch);
      Mat pred_batch;
      Forward(pred_batch, 1);      
      InitDeriv(labels_batch, trainerror_(epoch, batch));
      Backward();
      CalcWeights();      
      UpdateWeights(epoch, true); 
      if (params_.verbose_ == 2) {
        std::string info = std::string("Epoch: ") + std::to_string(epoch+1) +
                           std::string(", batch: ") + std::to_string(batch+1);
        mexPrintMsg(info);
      }      
    } // batch    
    if (params_.verbose_ == 1) {
      std::string info = std::string("Epoch: ") + std::to_string(epoch+1);
      mexPrintMsg(info);
    }
  } // epoch
  //mexPrintMsg("Training finished");
}

void Net::Classify(const mxArray *mx_data, mxArray *&mx_pred) {  
  //mexPrintMsg("Start classification...");
  ReadData(mx_data);
  InitActiv(data_);
  Mat pred;
  Forward(pred, 0);
  mx_pred = mexSetMatrix(pred);
  //mexPrintMsg("Classification finished");
}

void Net::InitNorm() {

  LayerInput *firstlayer = static_cast<LayerInput*>(layers_[0]);
  size_t num_weights = firstlayer->NumWeights();
  if (num_weights == 0) return;
  Mat norm_mat = data_;
  if (firstlayer->norm_ > 0) {
    norm_mat.Normalize(firstlayer->norm_);
  }
  Mat mean_vect = (Mean(norm_mat, 1) *= -1);
  if (firstlayer->is_mean_) {
    firstlayer->mean_weights_.get() = mean_vect;
    firstlayer->mean_weights_.get() += firstlayer->mean_;
  }
  if (firstlayer->is_maxdev_) {
    norm_mat.AddVect(mean_vect, 1);
    norm_mat *= norm_mat;
    Mat stdev_mat = Mean(norm_mat, 1).Sqrt();
    stdev_mat.CondAssign(stdev_mat, firstlayer->maxdev_, false, firstlayer->maxdev_);        
    Mat ones(stdev_mat.size1(), stdev_mat.size2());
    ones.assign(firstlayer->maxdev_);    
    firstlayer->maxdev_weights_.get() = (ones /= stdev_mat);
  }
}

void Net::InitActiv(const Mat &data) {
  mexAssert(layers_.size() >= 2 , "The net is not initialized");
  layers_[0]->activ_mat_.attach(data);
  layers_[0]->activ_mat_.Validate();
}

void Net::Forward(Mat &pred, int passnum) {
  //mexPrintMsg("Start forward pass...");  
  //mexPrintMsg("Forward pass for layer", layers_[0]->type_);  
  layers_[0]->Forward(NULL, passnum);  
  for (size_t i = 1; i < layers_.size(); ++i) {
    //mexPrintMsg("Forward pass for layer", layers_[]->type_);  
    Mat activ_mat_prev;
    layers_[i]->Forward(layers_[i-1], passnum);    
    if (layers_[i]->type_ == "c" || layers_[i]->type_ == "f") {
      layers_[i]->Nonlinear(passnum);            
    }    
    if (passnum == 0) layers_[i-1]->activ_mat_.clear();    
    if (utIsInterruptPending()) {
      Clear();
      mexAssert(false, "Ctrl-C Detected. END");
    }
    /*
    for (int j = 0; j < 5; ++j) {
      //mexPrintMsg("activ_mat_", layers_[i]->activ_mat_(0, j)); 
    }*/    
  }  
  pred.attach(layers_.back()->activ_mat_);  
  //("Forward pass finished");
}

void Net::Backward() {
  
  //mexPrintMsg("Start backward pass...");  
  size_t i;
  for (i = layers_.size() - 1; i > 0; --i) {    
    //mexPrintMsg("Backward pass for layer", layers_[i]->type_);    
    if (layers_[i]->type_ == "c" || layers_[i]->type_ == "f") {
      layers_[i]->Nonlinear(2);      
    }
    layers_[i]->Backward(layers_[i-1]);    
  }
  if (i == 0) {
    //mexPrintMsg("Backward pass for layer", layers_[0]->type_);  
    layers_[0]->Backward(NULL);    
  }
  //mexPrintMsg("Backward pass finished");  
}

void Net::InitDeriv(const Mat &labels_batch, ftype &loss) {  
  size_t batchsize = labels_batch.size1();
  size_t classes_num = labels_batch.size2();
  Layer *lastlayer = layers_.back();
  mexAssert(batchsize == lastlayer->batchsize_, 
    "The number of objects in data and label batches is different");
  mexAssert(classes_num == lastlayer->length_, 
    "Labels in batch and last layer must have equal number of classes");  
  if (lastlayer->function_ == "SVM") {
    Mat lossmat = lastlayer->activ_mat_;
    ((lossmat *= labels_batch) *= -1) += 1;
    lossmat.CondAssign(lossmat, 0, false, 0);
    lastlayer->deriv_mat_ = lossmat;
    (lastlayer->deriv_mat_ *= labels_batch) *= -2;    
    // correct loss also contains weightsT * weights / C, but it is too long to calculate it
    loss = (lossmat *= lossmat).Sum() / batchsize;    
  }
  else if (lastlayer->function_ == "soft" || lastlayer->function_ == "sigm" || lastlayer->function_ == "linear" || lastlayer->function_ == "relu" ) {
  
    lastlayer->deriv_mat_ = lastlayer->activ_mat_;
    lastlayer->deriv_mat_ -= labels_batch;    
    Mat lossmat = lastlayer->deriv_mat_;
    loss = (lossmat *= lastlayer->deriv_mat_).Sum() / (2 * batchsize);
  }
  lastlayer->deriv_mat_.MultVect(classcoefs_, 1);
  lastlayer->deriv_mat_.Validate(); 
}

void Net::CalcWeights() {  
  //mexPrintMsg("Start CalcWeights pass...");  
  //mexPrintMsg("CalcWeights pass for layer", layers_[0]->type_);
  layers_[0]->CalcWeights(NULL);
  for (size_t i = 1; i < layers_.size(); ++i) {
    //mexPrintMsg("CalcWeights pass for layer", layers_[i]->type_);    
    layers_[i]->CalcWeights(layers_[i-1]);
  }
  //mexPrintMsg("CalcWeights pass finished");  
}

void Net::UpdateWeights(size_t epoch, bool isafter) {
  weights_.Update(params_, epoch, isafter);  
}

void Net::ReadData(const mxArray *mx_data) {
  std::vector<size_t> data_dim = mexGetDimensions(mx_data);
  mexAssert(data_dim.size() == 4, "The data array must have 4 dimensions");  
  mexAssert(data_dim[0] == layers_[0]->mapsize_[0] && 
            data_dim[1] == layers_[0]->mapsize_[1],
    "Data and the first layer must have equal sizes");  
  mexAssert(data_dim[2] == layers_[0]->outputmaps_,
    "Data's 3rd dimension must be equal to the outputmaps on the input layer");
  mexAssert(data_dim[3] > 0, "Input data array is empty");
  ftype *data = mexGetPointer(mx_data);
  data_.attach(data, data_dim[3], data_dim[0] * data_dim[1] * data_dim[2]);  
}

void Net::ReadLabels(const mxArray *mx_labels) {
  std::vector<size_t> labels_dim = mexGetDimensions(mx_labels);  
  mexAssert(labels_dim.size() == 2, "The label array must have 2 dimensions");
  ////mexPrintMsg("labels_dim.", labels_dim[0]);
  ////mexPrintMsg("data_.size()", data_.size());  
  size_t classes_num = labels_dim[1];
  mexAssert(classes_num == layers_.back()->length_,
    "Labels and last layer must have equal number of classes");  
  labels_ = mexGetMatrix(mx_labels);
  classcoefs_.init(1, classes_num, 1);
  if (params_.balance_) {  
    Mat labels_mean = Mean(labels_, 1);
    for (size_t i = 0; i < classes_num; ++i) {
      mexAssert(labels_mean(i) > 0, "Balancing impossible: one of the classes is not presented");
      (classcoefs_(i) /= labels_mean(i)) /= classes_num;      
    }
  }
  if (layers_.back()->function_ == "SVM") {
    (labels_ *= 2) -= 1;    
  }
}

size_t Net::NumWeights() const {
  size_t num_weights = 0;
  for (size_t i = 0; i < layers_.size(); ++i) {    
    num_weights += layers_[i]->NumWeights();
  }
  return num_weights;
}

void Net::InitWeights(const mxArray *mx_weights_in) { // testing
  size_t num_weights = NumWeights();
  mexAssert(num_weights == mexGetNumel(mx_weights_in), 
    "In InitWeights the vector of weights has the wrong length!");
  weights_.Init(mexGetPointer(mx_weights_in), num_weights);
  size_t offset = 0;
  for (size_t i = 0; i < layers_.size(); ++i) {
    layers_[i]->InitWeights(weights_, offset, false);
  }
}

void Net::InitWeights(const mxArray *mx_weights_in, mxArray *&mx_weights) {
  size_t num_weights = NumWeights();
  bool isgen = false;
  if (mx_weights_in != NULL) { // training
    mexAssert(num_weights == mexGetNumel(mx_weights_in), 
      "In InitWeights the vector of weights has the wrong length!");
    mx_weights = mexDuplicateArray(mx_weights_in);    
  } else { // genweights
    mx_weights = mexNewMatrix(1, num_weights);    
    isgen = true;
  }
  weights_.Init(mexGetPointer(mx_weights), num_weights);
  size_t offset = 0;
  for (size_t i = 0; i < layers_.size(); ++i) {
    layers_[i]->InitWeights(weights_, offset, isgen);
  }  
}

void Net::GetTrainErrors(mxArray *&mx_errors) const {  
  mx_errors = mexSetMatrix(trainerror_);  
}

void Net::Clear() {
  for (size_t i = 0; i < layers_.size(); ++i){
    delete layers_[i];
  }
  layers_.clear();
  data_.clear();
  labels_.clear();
  trainerror_.clear();
  classcoefs_.clear(); 
}
