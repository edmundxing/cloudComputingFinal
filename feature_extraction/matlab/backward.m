function layers = backward(layers)

n = numel(layers);
batchsize = size(layers{1}.a, 4);

for l = n : -1 : 1
  if strcmp(layers{l}.type, 'c') || strcmp(layers{l}.type, 'f')
    if strcmp(layers{l}.function, 'soft') % for softmax
      layers{l}.d = softder(layers{l}.d, layers{l}.a);
    elseif strcmp(layers{l}.function, 'sigm') % for sigmoids
      layers{l}.d = layers{l}.d .* layers{l}.a .* (1 - layers{l}.a);  
    elseif strcmp(layers{l}.function, 'linear') % for linear
      layers{l}.d = layers{l}.d;    
    elseif strcmp(layers{l}.function, 'relu')
      layers{l}.d = layers{l}.d .* (layers{l}.a > 0);          
    elseif strcmp(layers{l}.function, 'SVM') % for SVM
    end;
    layers{l}.d(-layers{l}.eps < layers{l}.d & layers{l}.d < layers{l}.eps) = 0;    
  end;
  
  if strcmp(layers{l}.type, 'n')    
    layers{l-1}.d = layers{l}.d;
    if (layers{l}.is_dev == 1)
      layers{l-1}.d = layers{l-1}.d .* repmat(layers{l}.w(:, :, :, 2), [1 1 1 batchsize]);
    end;

  elseif strcmp(layers{l}.type, 'c')
    d_cur = layers{l}.d;    
    if (layers{l}.padding(1) > 0 || layers{l}.padding(2) > 0)
      ds = size(layers{l}.d); ds(end+1:4) = 1;
      padding = layers{l}.kernelsize - 1 - layers{l}.padding;
      d_cur = zeros([ds(1:2) + 2*padding ds(3:4)]);
      d_cur(padding(1)+1:padding(1)+ds(1), padding(2)+1:padding(2)+ds(2), :, :) =  layers{l}.d;      
    end;
    layers{l-1}.d = zeros(size(layers{l-1}.a));
    for i = 1 : layers{l}.outputmaps        
      for j = 1 : layers{l-1}.outputmaps
        if (layers{l}.padding(1) > 0 || layers{l}.padding(2) > 0)
          layers{l-1}.d(:, :, j, :) = layers{l-1}.d(:, :, j, :) + ...
            filtn(d_cur(:, :, i, :), layers{l}.k(:, :, j, i), 'valid');
        else
          layers{l-1}.d(:, :, j, :) = layers{l-1}.d(:, :, j, :) + ...
            filtn(d_cur(:, :, i, :), layers{l}.k(:, :, j, i), 'full');          
        end;
      end        
    end;

  elseif strcmp(layers{l}.type, 's')    
    sc = [layers{l}.scale 1 1];
    st = [layers{l}.stride 1 1];
    targsize = layers{l-1}.mapsize;
    curder = expand(layers{l}.d, sc);
    if strcmp(layers{l}.function, 'max')
      curval = expand(layers{l}.a, sc);
      if (~isequal(sc, st))
        prevval = stretch(layers{l-1}.a, sc, st);
        maxmat = (prevval == curval);
        maxmat = uniq(maxmat, sc);
        curder = curder .* maxmat;
        layers{l-1}.d = shrink(curder, sc, st);
      else
        maxmat = (layers{l-1}.a == curval);
        maxmat = uniq(maxmat, sc);
        layers{l-1}.d = curder .* maxmat;
      end;
    elseif strcmp(layers{l}.function, 'mean')
      curder = curder / prod(sc);
      if (~isequal(sc, st))
        curder = shrink(curder, sc, st);
      end;
      layers{l-1}.d = curder;
    end;
    ind = (layers{l}.mapsize - 1) .* st(1:2);      
    realnum = targsize - ind;
    if (sc(1) > realnum(1))        
      extra = sum(layers{l-1}.d(targsize(1)+1:end, :, :, :), 1) / realnum(1);        
      layers{l-1}.d(targsize(1)+1:end, :, :, :) = [];
      layers{l-1}.d(ind(1)+1 : targsize(1), :, :, :) = ...
        layers{l-1}.d(ind(1)+1 : targsize(1), :, :, :) + ...
        repmat(extra, [realnum(1) 1 1 1]);
    end;
    if (sc(2) > realnum(2))
      extra = sum(layers{l-1}.d(:, targsize(2)+1:end, :, :), 2) / realnum(2);
      layers{l-1}.d(:, targsize(2)+1:end, :, :) = [];
      layers{l-1}.d(:, ind(2)+1 : targsize(2), :, :) = ...
        layers{l-1}.d(:, ind(2)+1 : targsize(2), :, :) + ...
        repmat(extra, [1 realnum(2) 1 1]);
    end;      

  elseif strcmp(layers{l}.type, 'f')
    if (layers{l}.dropout > 0) % dropout
      layers{l}.di = maskprod(layers{l}.d, 0, layers{l}.w, 0, layers{l}.dropmat);
    else
      layers{l}.di = layers{l}.d * layers{l}.w; 
    end;
    if ~strcmp(layers{l-1}.type, 'f')        
      mapsize = layers{l-1}.mapsize;
      d_trans = reshape(layers{l}.di, [batchsize mapsize(2) mapsize(1) layers{l-1}.outputmaps]);
      layers{l-1}.d = permute(d_trans, [3 2 4 1]);        
    else
      layers{l-1}.d = layers{l}.di;
    end;      
  end;  
  if (l == 1), ind = 1; else ind = l-1; end;  
  layers{ind}.d(-layers{ind}.eps < layers{ind}.d & layers{ind}.d < layers{ind}.eps) = 0;
end
    
end
