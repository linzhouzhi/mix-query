/**
 * Created by lzz on 17/5/2.
 */


/**
 * 设置 uuid
 */
function set_cookie(key, value)
{
    var Days = 360;
    var exp = new Date();
    exp.setTime(exp.getTime() + Days*24*60*60*1000);
    document.cookie = key+"="+ escape ( value ) + ";expires=" + exp.toGMTString() + ";path=/";
}

/**
 * 获取 cookie
 * @param name
 * @returns {null}
 */
function get_cookie(name)
{
    var arr,reg=new RegExp("(^| )"+name+"=([^;]*)(;|$)");
    if(arr=document.cookie.match(reg))
        return unescape(arr[2]);
    else
        return null;
}