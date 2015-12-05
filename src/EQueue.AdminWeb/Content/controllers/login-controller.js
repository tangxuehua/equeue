function LoginController($scope, $http, $window) {
    $scope.errorMsg = '';
    $scope.returnUrl = returnUrl;
    $scope.loginAccount = {
        accountName: '',
        password: '',
        rememberMe: 'False'
    };

    $scope.submit = function () {
        if (isStringEmpty($scope.loginAccount.accountName)) {
            $scope.errorMsg = '请输入账号。';
            return false;
        }
        if (isStringEmpty($scope.loginAccount.password)) {
            $scope.errorMsg = '请输入密码。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/account/login',
            data: $scope.loginAccount
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                if (isStringEmpty($scope.returnUrl)) {
                    $window.location.href = '/home/index';
                }
                else {
                    $window.location.href = $scope.returnUrl;
                }
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };
}