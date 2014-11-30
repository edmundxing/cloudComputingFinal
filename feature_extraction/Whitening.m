function [ Y ] = Whitening( X , epsilon)
    if ~exist('epsilon','var')
        epsilon = 0.0001;
    end
    [N D] = size(X);
     % Compute the regularized scatter matrix.
    scatter = (X'*X + epsilon*eye(D));
    % The epsilon corresponds to virtual data.
    N = N + epsilon;
    % Take the eigendecomposition of the scatter matrix.
    [V D] = eig(scatter);
    % This is pretty hacky, but we don't want to divide by tiny
    % eigenvalues, so make sure they're all of reasonable size.
    D = max(diag(D), epsilon);
    % Now use the eigenvalues to find the root-inverse of the
    % scatter matrix.	
    irD = diag(1./sqrt(D));
    % Reassemble into the transformation matrix.
    W = sqrt(N-1) * V * irD * V';
    % Apply to the data.
    Y = X*W;
    
end

