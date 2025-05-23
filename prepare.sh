set -eu

conan_major=`conan --version | awk '{print $3}' | awk -F'.' '{print $1}'`
echo "Using conan major version: ${conan_major}"

echo -n "Exporting custom recipes..."

if [ $conan_major -eq 1 ] ; then
    echo -n "folly."
    conan export 3rd_party/folly folly/nu2.2023.12.18.00@ >/dev/null
    echo -n "userpace rcu."
    conan export 3rd_party/userspace-rcu userspace-rcu/nu2.0.14.0@ >/dev/null
else
    echo -n "folly."
    conan export 3rd_party/folly --name folly --version nu2.2023.12.18.00 >/dev/null
    echo -n "userspace rcu."
    conan export 3rd_party/userspace-rcu --name userspace-rcu --version nu2.0.14.0 >/dev/null
fi

echo "done."