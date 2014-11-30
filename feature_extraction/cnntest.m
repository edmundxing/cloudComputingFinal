function [err, bad, layers_forward, pred] = cnntest(layers, weights, x, y, type)

[layers_forward, pred] = cnnclassify(layers, weights, x, type);

[~, pred_ind] = max(pred, [], 2);
[~, y_ind] = max(y, [], 2);
bad = find(pred_ind ~= y_ind);
err = length(bad) / size(pred_ind, 1);

%bad = [];
%err = sum((pred - y).^2) / 2;

end
