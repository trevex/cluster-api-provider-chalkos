# The images the KubeVirt lab runs, built from the chalkos this flake pins:
#
# - lab-image is the chalkos Kubernetes image for VMs, as the directory mkImages builds, with its
#   OCI layout as lab-image.oci. The providers read the layout's header and contract from a
#   registry, and the nodes pull it from there when they install in place.
# - lab-disk is a KubeVirt containerDisk of the same image: an OCI image layout whose one layer
#   holds the image's raw disk as /disk/chalkos.qcow2. CDI imports it into each VM's DataVolume.
#
# The image carries no OS CA, so a machine in maintenance mode accepts any cluster's secrets on
# first contact. That suits a lab on a private network only.
{ chalkos, pkgs }:
let
  images = chalkos.lib.mkImages {
    images = [
      {
        name = "kubernetes-kvm";
        platform = "kvm";
        modules = [ chalkos.nixosModules.kubernetes ];
      }
    ];
  };
  image = images.kubernetes-kvm;
in
{
  lab-image = image;

  lab-image-oci = image.oci;

  # The layer is an uncompressed tar of disk/chalkos.qcow2, owned by 107:107, the qemu user that
  # KubeVirt runs a containerDisk's VM as. The layout is written straight into $out, so the
  # multi-gigabyte disk never passes through the build directory.
  lab-disk =
    pkgs.runCommand "chalkos-kubernetes-kvm-disk"
      {
        nativeBuildInputs = [
          pkgs.qemu-utils
          pkgs.gnutar
          pkgs.jq
        ];
      }
      ''
        mkdir -p $out/blobs/sha256 $out/stage/disk
        qemu-img convert -f raw -O qcow2 ${image}/chalkos.raw $out/stage/disk/chalkos.qcow2
        tar --sort=name --mtime=@0 --owner=107 --group=107 --numeric-owner \
          -cf $out/layer.tar -C $out/stage disk
        rm -rf $out/stage

        layer=$(sha256sum $out/layer.tar | cut -d' ' -f1)
        layerSize=$(stat -c %s $out/layer.tar)
        mv $out/layer.tar $out/blobs/sha256/$layer

        jq -cn --arg diff "sha256:$layer" '{
          architecture: "amd64",
          os: "linux",
          config: {},
          rootfs: {type: "layers", diff_ids: [$diff]}
        }' > config.json
        config=$(sha256sum config.json | cut -d' ' -f1)
        configSize=$(stat -c %s config.json)
        mv config.json $out/blobs/sha256/$config

        jq -cn --arg config "sha256:$config" --argjson configSize $configSize \
          --arg layer "sha256:$layer" --argjson layerSize $layerSize '{
          schemaVersion: 2,
          mediaType: "application/vnd.oci.image.manifest.v1+json",
          config: {mediaType: "application/vnd.oci.image.config.v1+json", digest: $config, size: $configSize},
          layers: [{mediaType: "application/vnd.oci.image.layer.v1.tar", digest: $layer, size: $layerSize}]
        }' > manifest.json
        manifest=$(sha256sum manifest.json | cut -d' ' -f1)
        manifestSize=$(stat -c %s manifest.json)
        mv manifest.json $out/blobs/sha256/$manifest

        jq -cn --arg manifest "sha256:$manifest" --argjson manifestSize $manifestSize '{
          schemaVersion: 2,
          manifests: [{mediaType: "application/vnd.oci.image.manifest.v1+json", digest: $manifest, size: $manifestSize}]
        }' > $out/index.json
        echo '{"imageLayoutVersion":"1.0.0"}' > $out/oci-layout
      '';
}
