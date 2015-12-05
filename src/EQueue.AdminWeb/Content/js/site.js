//消息弹出框
var msg = function (message) {
    bootbox.alert(message);
};
//弹出提示消息框，支持回调函数
var showMessage = function (message, callback) {
    bootbox.alert(message, function (result) {
        callback();
    });
};
//确认对话框
var showConfirm = function (message, callback) {
    bootbox.confirm(message, function (result) {
        if (result) {
            callback();
        }
    });
};

String.prototype.trim = function () {
    return this.replace(/(^\s*)|(\s*$)/g, "");
}

var isStringEmpty = function (input) {
    if (input == null || input == '' || input.trim() == '') {
        return true;
    }
    return false;
};

function showAutoCloseAlert() {
    $('#alert_placeholder').append('<div class="alert alert-success fade in" id="success-alert"><button type="button" class="close" data-dismiss="alert">&times;</button><strong>Success! </strong>Please refresh the page after a few seconds to see the changes.</div>');
    setTimeout(function () {
        $('#alert_placeholder').children('.alert:first-child').remove();
    }, 2000);
}