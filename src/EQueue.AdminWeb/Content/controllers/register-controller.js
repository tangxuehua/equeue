function RegisterController($scope, $http, $window) {
    $scope.errorMsg = '';
    $scope.newAccount = {
        accountName: '',
        password: '',
        confirmPassword: ''
    };

    $scope.submit = function () {
        if (isStringEmpty($scope.newAccount.accountName)) {
            $scope.errorMsg = '请输入账号。';
            return false;
        }
        if (isStringEmpty($scope.newAccount.password)) {
            $scope.errorMsg = '请输入密码。';
            return false;
        }
        if (isStringEmpty($scope.newAccount.confirmPassword)) {
            $scope.errorMsg = '请输入密码确认。';
            return false;
        }
        if ($scope.newAccount.password != $scope.newAccount.confirmPassword) {
            $scope.errorMsg = '密码输入不一致。';
            return false;
        }

        $http({
            method: 'POST',
            url: '/account/register',
            data: $scope.newAccount
        })
        .success(function (result, status, headers, config) {
            if (result.success) {
                $window.location.href = '/home/index';
            } else {
                $scope.errorMsg = result.errorMsg;
            }
        })
        .error(function (result, status, headers, config) {
            $scope.errorMsg = result.errorMsg;
        });
    };
}